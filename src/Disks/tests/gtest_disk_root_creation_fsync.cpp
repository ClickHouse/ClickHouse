/// Covers the durability of a newly created disk root, for the shapes that SQL cannot reach:
/// a relative root, a root spelled with `..` after a symlink, a root that resolves to a regular
/// file, an unreadable parent, an empty encrypted prefix, and which directory is fsynced - a
/// count alone cannot distinguish the created level from the parent that holds its entry.

#include <gtest/gtest.h>

#include <Common/ProfileEvents.h>
#include <Disks/DiskEncrypted.h>
#include <Disks/DiskLocal.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteHelpers.h>
#include <Poco/TemporaryFile.h>
#include <base/scope_guard.h>

#include <unistd.h> /// for ::geteuid

#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event DirectorySync;
}

using namespace DB;

namespace
{

ProfileEvents::Count directorySyncs()
{
    return ProfileEvents::global_counters[ProfileEvents::DirectorySync];
}

class DiskRootCreationFsyncTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
    }

    String base() const { return temp_dir->path() + "/"; }

    /// Starts the disk and returns how many directories were fsync'd while doing so.
    static ProfileEvents::Count startupAndCountSyncs(DiskPtr disk)
    {
        auto before = directorySyncs();
        disk->startup(/* skip_access_check= */ true);
        return directorySyncs() - before;
    }

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
};

}

/// Every level created for the root contributes exactly one fsync, of the directory holding it.
TEST_F(DiskRootCreationFsyncTest, EachCreatedLevelIsSyncedOnce)
{
    auto disk = std::make_shared<DiskLocal>("test_disk", base() + "a/b/c/");
    EXPECT_EQ(startupAndCountSyncs(disk), 3);
    EXPECT_TRUE(fs::is_directory(base() + "a/b/c"));
    EXPECT_FALSE(disk->isBroken());
}

/// An already existing root costs nothing: the disk's second startup does not fsync at all.
TEST_F(DiskRootCreationFsyncTest, ExistingRootIsNotSynced)
{
    auto path = base() + "existing/";
    fs::create_directories(path);

    auto disk = std::make_shared<DiskLocal>("test_disk", path);
    EXPECT_EQ(startupAndCountSyncs(disk), 0);
    EXPECT_FALSE(disk->isBroken());
}

/// A disk root is not required to be absolute, so the walk must be anchored: without that the
/// topmost level has an empty parent, which cannot be opened.
TEST_F(DiskRootCreationFsyncTest, RelativeRootIsSynced)
{
    /// Other tests resolve relative paths against the process working directory, so it has to be
    /// restored on every exit path - including a throw from the filesystem calls below.
    auto previous_working_directory = fs::current_path();
    SCOPE_EXIT({ fs::current_path(previous_working_directory); });
    fs::current_path(base());

    auto disk = std::make_shared<DiskLocal>("test_disk", "relative/root/");
    EXPECT_EQ(startupAndCountSyncs(disk), 2);
    EXPECT_TRUE(fs::is_directory(base() + "relative/root"));
    EXPECT_FALSE(disk->isBroken());
}

/// A root spelled with `..` after a symlink must be fsync'd where the kernel actually created the
/// directory: resolving `..` lexically instead would name a path that does not even exist, and the
/// deepest level's parent would then be missing (one fsync instead of two).
TEST_F(DiskRootCreationFsyncTest, SymlinkBeforeDotDotIsSynced)
{
    fs::create_directories(base() + "target/sub");
    fs::create_directory_symlink("target/sub", base() + "link");

    /// `link/..` is the directory holding `sub`, i.e. `target`.
    auto disk = std::make_shared<DiskLocal>("test_disk", base() + "link/../deep/root/");
    EXPECT_EQ(startupAndCountSyncs(disk), 2);
    EXPECT_TRUE(fs::is_directory(base() + "target/deep/root"));
    EXPECT_FALSE(fs::exists(base() + "deep"));
    EXPECT_FALSE(disk->isBroken());
}

/// A root that resolves to a regular file must still fail the way it does without any fsync
/// handling, i.e. the disk must end up broken rather than silently healthy and unusable.
TEST_F(DiskRootCreationFsyncTest, RootResolvingToRegularFileBreaksTheDisk)
{
    auto file_path = base() + "as_file";
    {
        WriteBufferFromFile out(file_path);
        writeString("not a directory", out);
        out.finalize();
    }

    for (const auto & spelling : {file_path, file_path + "/", file_path + "/sub/"})
    {
        auto disk = std::make_shared<DiskLocal>("test_disk", spelling);
        disk->startup(/* skip_access_check= */ true);
        EXPECT_TRUE(disk->isBroken()) << "root spelled as " << spelling;
    }
}

/// What is synchronized must be the directory that HOLDS a created level's entry, not the level
/// itself - counting the fsyncs cannot tell the two apart, because either way there is one per
/// created level. Opening for fsync needs read permission while creating a subdirectory does not,
/// so a write-and-search-only topmost parent is skipped: exactly the levels below it are counted.
/// Synchronizing the levels themselves would instead find all three of them freshly readable.
TEST_F(DiskRootCreationFsyncTest, SyncTargetIsTheOwningParent)
{
    /// Running as root bypasses permission bits, so EACCES would never trigger.
    if (::geteuid() == 0)
        GTEST_SKIP() << "must not run as root (permission checks are bypassed)";

    auto hardened = base() + "hardened/";
    fs::create_directories(hardened);
    fs::permissions(hardened, fs::perms::owner_exec | fs::perms::owner_write);
    SCOPE_EXIT({ fs::permissions(hardened, fs::perms::owner_all); });

    auto disk = std::make_shared<DiskLocal>("test_disk", hardened + "a/b/c/");
    /// `a/b` and `a` are synchronized; `hardened`, which holds `a`, cannot be opened.
    EXPECT_EQ(startupAndCountSyncs(disk), 2);
    EXPECT_TRUE(fs::is_directory(hardened + "a/b/c"));
    EXPECT_FALSE(disk->isBroken());
}

/// A parent that is searchable but not readable cannot be opened for fsync. Creating the root there
/// still works, so the disk must start normally instead of failing.
TEST_F(DiskRootCreationFsyncTest, UnreadableParentDoesNotBreakTheDisk)
{
    /// Running as root bypasses permission bits, so EACCES would never trigger.
    if (::geteuid() == 0)
        GTEST_SKIP() << "must not run as root (permission checks are bypassed)";

    auto parent = base() + "unreadable/";
    fs::create_directories(parent);
    fs::permissions(parent, fs::perms::owner_exec | fs::perms::owner_write);

    auto disk = std::make_shared<DiskLocal>("test_disk", parent + "root/");
    auto syncs = startupAndCountSyncs(disk);
    auto broken = disk->isBroken();

    fs::permissions(parent, fs::perms::owner_all);

    EXPECT_EQ(syncs, 0);
    EXPECT_FALSE(broken);
    EXPECT_TRUE(fs::is_directory(parent + "root"));
}

#if USE_SSL
namespace
{

DiskPtr makeEncryptedDisk(DiskPtr wrapped_disk, const String & prefix)
{
    static constexpr auto key = "1234567812345678";
    auto settings = std::make_unique<DiskEncryptedSettings>();
    settings->wrapped_disk = std::move(wrapped_disk);
    settings->disk_path = prefix;
    settings->current_algorithm = FileEncryption::Algorithm::AES_128_CTR;
    settings->current_key = key;
    settings->current_key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
    settings->all_keys[settings->current_key_fingerprint] = key;
    return std::make_shared<DiskEncrypted>("test_encrypted_disk", std::move(settings));
}

/// A disk shaped like DiskObjectStorage: reports itself remote and has no directory sync guard.
/// Counts the directory lookups an encrypted disk performs on it, which is what makes the syncs a
/// remote delegate can never do observable as the round-trips they would cost.
class RemoteDiskWithoutSyncGuard : public DiskLocal
{
public:
    using DiskLocal::DiskLocal;

    bool isRemote() const override { return true; }
    SyncGuardPtr getDirectorySyncGuard(const String &) const override { return nullptr; }

    bool existsDirectory(const String & path) const override
    {
        ++directory_lookups;
        return DiskLocal::existsDirectory(path);
    }

    mutable size_t directory_lookups = 0;
};

/// Records which directory each fsync targets and still performs it. Counting the fsyncs cannot
/// tell "synchronized the directory holding the new entry" from "synchronized the new directory
/// itself" - both produce one event per created level - so the paths have to be asserted.
class SyncPathRecordingDisk : public DiskLocal
{
public:
    using DiskLocal::DiskLocal;

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override
    {
        synced_paths.push_back(path);
        return DiskLocal::getDirectorySyncGuard(path);
    }

    mutable std::vector<String> synced_paths;
};

}

/// The prefix of an encrypted disk lives inside the wrapped disk, so it is the wrapped disk that
/// has to synchronize the directories holding the created levels - including its own root, which
/// holds the entry of a one-level prefix. The recorded paths are what pins the target: each is the
/// directory that OWNS a created level's entry, never the created level itself.
TEST_F(DiskRootCreationFsyncTest, EncryptedPrefixIsSynced)
{
    auto wrapped_disk = std::make_shared<SyncPathRecordingDisk>("test_disk", base());
    auto & synced_paths = wrapped_disk->synced_paths;

    /// The wrapped disk's own root (the empty path) holds the entry of a one-level prefix.
    auto before_one_level = directorySyncs();
    makeEncryptedDisk(wrapped_disk, "one/");
    EXPECT_EQ(directorySyncs() - before_one_level, 1);
    EXPECT_EQ(synced_paths, std::vector<String>({""}));
    EXPECT_TRUE(fs::is_directory(base() + "one"));

    /// Deepest first: `nested` holds `prefix`, the wrapped disk's root holds `nested`.
    synced_paths.clear();
    auto before_nested = directorySyncs();
    makeEncryptedDisk(wrapped_disk, "nested/prefix/");
    EXPECT_EQ(directorySyncs() - before_nested, 2);
    EXPECT_EQ(synced_paths, std::vector<String>({"nested", ""}));
    EXPECT_TRUE(fs::is_directory(base() + "nested/prefix"));

    /// The prefix may be the wrapped disk's root itself, which always exists already.
    synced_paths.clear();
    auto before_empty = directorySyncs();
    makeEncryptedDisk(wrapped_disk, "");
    EXPECT_EQ(directorySyncs() - before_empty, 0);
    EXPECT_TRUE(synced_paths.empty());

    /// An existing prefix costs nothing.
    synced_paths.clear();
    auto before_existing = directorySyncs();
    makeEncryptedDisk(wrapped_disk, "nested/prefix/");
    EXPECT_EQ(directorySyncs() - before_existing, 0);
    EXPECT_TRUE(synced_paths.empty());
}

/// A remote wrapped disk cannot synchronize a directory, so the missing levels must not even be
/// looked up: every lookup would be a remote round-trip that could only ever produce zero syncs.
TEST_F(DiskRootCreationFsyncTest, RemoteWrappedDiskIsNotProbed)
{
    auto wrapped_disk = std::make_shared<RemoteDiskWithoutSyncGuard>("test_remote_disk", base());

    auto before = directorySyncs();
    makeEncryptedDisk(wrapped_disk, "nested/prefix/");

    EXPECT_EQ(wrapped_disk->directory_lookups, 0);
    EXPECT_EQ(directorySyncs() - before, 0);
    EXPECT_TRUE(fs::is_directory(base() + "nested/prefix"));
}
#endif
