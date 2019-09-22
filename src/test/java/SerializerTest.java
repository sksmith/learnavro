import lombok.extern.slf4j.Slf4j;
import me.seanksmith.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class SerializerTest {
    @Test
    void trySerialization() throws IOException {
        List<User> users = createUsers();

        File file = new File("users.avro");

        serializeUsers(users, file);
        List<User> dusers = deserializeUsers(file);

        for (User user : users) {
            boolean found = false;
            for (User duser : dusers) {
                if (duser.equals(user)) {
                    found = true;
                    break;
                }
            }

            assertTrue(found, "Unable to find user: " + user);
        }

        if (!file.delete()) {
            log.error("Unable to delete generated file");
        }
    }

    private static void serializeUsers(List<User> users, File file) throws IOException {
        DatumWriter<User> dw = new SpecificDatumWriter<>(User.class);

        try (DataFileWriter<User> dfw = new DataFileWriter<>(dw)) {
            dfw.create(User.getClassSchema(), file);

            for (User user : users) {
                dfw.append(user);
            }
        }
    }

    private static List<User> deserializeUsers(File file) throws IOException {
        DatumReader<User> dr = new SpecificDatumReader<>(User.class);
        List<User> users = new ArrayList<>();

        try (DataFileReader<User> dfr = new DataFileReader<User>(file, dr)) {
            while (dfr.hasNext()) {
                users.add(dfr.next());
            }
        }

        return users;
    }

    /**
     * Creates users for tests, and also demonstrates various ways to create a user
     *
     * @return a list of valid test users
     */
    private static List<User> createUsers() {
        User user = new User();
        user.setName("Alyssa");
        user.setFavoriteNumber(256);
        // Leave favorite color null
        List<User> users = new ArrayList<>();
        users.add(user);

        // Alternate constructor
        user = new User("Ben", 7, "red");
        users.add(user);

        // Construct via builder
        user = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();
        users.add(user);

        return users;
    }
}
