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

TEST(DiskAccessStorageShutdown, DoesNotWaitOutTheListsWritingTimeout)
{
    Poco::TemporaryFile temp_dir;
    temp_dir.createDirectories();
    String dir = temp_dir.path() + "/";

    AccessChangesNotifier notifier;
    DiskAccessStorage storage("test_disk", dir, notifier, /*readonly_=*/false, /*allow_backup_=*/false);

    auto user = std::make_shared<User>();
    user->setName("alice");
    storage.insert(user);

    /// Shutting down right after the insert is the interleaving in which the request to stop the
    /// background lists-writing thread is made before that thread starts waiting for it. Writing
    /// the lists takes milliseconds, while the thread's batching timeout is a minute.
    auto started = std::chrono::steady_clock::now();
    storage.shutdown();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();

    EXPECT_LT(elapsed_ms, 20000) << "shutdown() took " << elapsed_ms << " ms";

    /// The lists are on disk once `shutdown` returns, whichever thread wrote them.
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(dir) / "need_rebuild_lists.mark"));
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(dir) / "users.list"));
}
