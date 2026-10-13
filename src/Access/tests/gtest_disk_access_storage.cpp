#include <gtest/gtest.h>

#include <Access/AccessChangesNotifier.h>
#include <Access/AccessEntityIO.h>
#include <Access/DiskAccessStorage.h>
#include <Access/User.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>

#include <Poco/TemporaryFile.h>

#include <chrono>
#include <filesystem>
#include <fstream>


using namespace DB;

namespace
{

void writeEntityToFile(const std::filesystem::path & file_path, const IAccessEntity & entity)
{
    std::ofstream out(file_path);
    out << serializeAccessEntity(entity);
}

void writeNeedRebuildMarker(const String & directory)
{
    std::ofstream{directory + "need_rebuild_lists.mark"};
}

std::shared_ptr<User> makeUser(const String & name)
{
    auto user = std::make_shared<User>();
    user->setName(name);
    return user;
}

}

TEST(AccessEntityIO, RejectsQueryParameters)
{
    EXPECT_THROW(deserializeAccessEntity("ATTACH ROLE {r:Identifier};"), Exception);
    EXPECT_THROW(deserializeAccessEntity("ATTACH USER {u:Identifier};"), Exception);
    EXPECT_THROW(deserializeAccessEntity("ATTACH USER u1 IDENTIFIED WITH plaintext_password BY {pw:String};"), Exception);
    EXPECT_THROW(deserializeAccessEntity("ATTACH USER u1 VALID UNTIL {ts:String};"), Exception);
    EXPECT_THROW(deserializeAccessEntity("ATTACH USER u1 DEFAULT ROLE {r:Identifier};"), Exception);
    EXPECT_THROW(deserializeAccessEntity("ATTACH ROLE r1; ATTACH GRANT SELECT ON *.* TO {g:Identifier};"), Exception);
}

TEST(DiskAccessStorageRecovery, RebuildRemovesTempFiles)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";

    auto stranded_tmp = std::filesystem::path(dir) / (toString(UUIDHelpers::generateV4()) + ".tmp");
    std::ofstream{stranded_tmp} << "text";

    writeNeedRebuildMarker(dir);

    AccessChangesNotifier notifier;
    DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);

    EXPECT_FALSE(std::filesystem::exists(stranded_tmp));
}

TEST(DiskAccessStorageRecovery, RebuildRemovesOlderDuplicates)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";

    auto user_a = std::make_shared<User>();
    user_a->setName("alice");
    UUID id_a = UUIDHelpers::generateV4();
    auto path_a = std::filesystem::path(dir) / (toString(id_a) + ".sql");
    writeEntityToFile(path_a, *user_a);

    auto user_b = std::make_shared<User>();
    user_b->setName("alice");
    UUID id_b = UUIDHelpers::generateV4();
    auto path_b = std::filesystem::path(dir) / (toString(id_b) + ".sql");
    writeEntityToFile(path_b, *user_b);

    /// Make `path_a` older than `path_b` so the deduplication logic keeps `id_b` and remove `id_a`.
    auto now = std::filesystem::file_time_type::clock::now();
    std::filesystem::last_write_time(path_a, now - std::chrono::seconds(10));
    std::filesystem::last_write_time(path_b, now);

    writeNeedRebuildMarker(dir);

    AccessChangesNotifier notifier;
    DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);

    EXPECT_FALSE(std::filesystem::exists(path_a));
    EXPECT_TRUE(std::filesystem::exists(path_b));

    auto resolved = storage.find<User>("alice");
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(*resolved, id_b);
}

TEST(DiskAccessStorageRecovery, KeepsUserCreatedAfterReloadOnCrash)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";
    Poco::TemporaryFile crashed_dir; /// Only the path; `std::filesystem::copy` creates the directory.

    AccessChangesNotifier notifier;
    {
        DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);
        storage.insert(makeUser("alice")); /// Starts the lists-writing thread, which waits a minute.
        storage.reload(IAccessStorage::ReloadMode::ALL); /// Writes the lists and removes need_rebuild_lists.mark.
        storage.insert(makeUser("bob"));
        std::filesystem::copy(dir, crashed_dir.path(), std::filesystem::copy_options::recursive);
    }

    DiskAccessStorage restarted("test_disk", crashed_dir.path() + "/", notifier, /*readonly_=*/false, /*allow_backup_=*/false);
    EXPECT_TRUE(restarted.find<User>("alice").has_value());
    EXPECT_TRUE(restarted.find<User>("bob").has_value());
}

TEST(DiskAccessStorageRecovery, KeepsUserCreatedAfterFailedListWrite)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";
    const std::filesystem::path users_list = dir + "users.list";
    const std::filesystem::path saved_users_list = dir + "users.list.saved";

    AccessChangesNotifier notifier;
    {
        DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);
        std::filesystem::rename(users_list, saved_users_list);
        std::filesystem::create_directory(users_list);
        storage.reload(IAccessStorage::ReloadMode::ALL); /// Fails to write `users.list`.
        storage.insert(makeUser("bob"));
    }
    std::filesystem::remove(users_list);
    std::filesystem::rename(saved_users_list, users_list);

    DiskAccessStorage restarted("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);
    EXPECT_TRUE(restarted.find<User>("bob").has_value());
}

TEST(DiskAccessStorageRecovery, KeepsReloadedUserAfterFailedListWrite)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";
    const std::filesystem::path users_list = dir + "users.list";
    const std::filesystem::path saved_users_list = dir + "users.list.saved";

    AccessChangesNotifier notifier;
    {
        DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);
        writeEntityToFile(std::filesystem::path(dir) / (toString(UUIDHelpers::generateV4()) + ".sql"), *makeUser("carol"));
        std::filesystem::rename(users_list, saved_users_list);
        std::filesystem::create_directory(users_list);
        storage.reload(IAccessStorage::ReloadMode::ALL); /// Loads `carol`, fails to write `users.list`.
        EXPECT_TRUE(storage.find<User>("carol").has_value());
    }
    std::filesystem::remove(users_list);
    std::filesystem::rename(saved_users_list, users_list);

    DiskAccessStorage restarted("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);
    EXPECT_TRUE(restarted.find<User>("carol").has_value());
}

TEST(DiskAccessStorageRecovery, KeepsUserWhenMarkCannotBeCreated)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";
    UUID id = UUIDHelpers::generateV4();
    auto path = std::filesystem::path(dir) / (toString(id) + ".sql");
    writeEntityToFile(path, *makeUser("alice"));

    AccessChangesNotifier notifier;
    DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);
    std::filesystem::create_directory(dir + "need_rebuild_lists.mark"); /// The mark cannot be created as a file.

    EXPECT_THROW(storage.remove(id), Exception);
    EXPECT_TRUE(storage.find<User>("alice").has_value());
    EXPECT_TRUE(std::filesystem::exists(path));

    EXPECT_NO_THROW(storage.reload(IAccessStorage::ReloadMode::ALL));
    EXPECT_TRUE(storage.find<User>("alice").has_value());
}
