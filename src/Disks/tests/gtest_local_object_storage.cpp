#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <unistd.h> /// for ::getpid

#include <algorithm> /// for std::sort
#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace
{

/// Build a LocalObjectStorage rooted at `key_prefix` (directory must exist).
DB::ObjectStoragePtr makeLocalObjectStorage(const std::string & key_prefix)
{
    DB::LocalObjectStorageSettings settings(
        /*disk_name_=*/"test_local",
        /*key_prefix_=*/key_prefix,
        /*read_only_=*/false);
    return std::make_shared<DB::LocalObjectStorage>(std::move(settings));
}

/// A scoped temp directory that cleans itself up on destruction.
struct ScopedTempDir
{
    fs::path path;
    explicit ScopedTempDir(const std::string & name_hint)
        : path(fs::temp_directory_path() / fs::path(name_hint + "_" + std::to_string(::getpid())))
    {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

}


/// `LocalObjectStorage::listObjects` must not follow symlinks into a cycle
/// (otherwise it can blow the stack via unbounded recursion). This test
/// creates two self-referential symlinks — `<root>/loop -> .` (inside the
/// root) and `<root>/sub/back -> ..` (inside a nested directory back to the
/// root) — both of which would cycle forever if symlinks were followed.
/// It then verifies that `listObjects` terminates and returns only the real
/// files on disk.
TEST(LocalObjectStorage, ListObjectsDoesNotFollowSymlinkCycles)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_cycle");
    const auto & root = tmp.path;

    /// Lay out:
    ///   root/
    ///     real.txt            ← regular file
    ///     sub/nested.txt      ← regular file at depth 1
    ///     loop -> .           ← symlink cycle targeting root
    ///     sub/back -> ..      ← another symlink cycle targeting root
    fs::create_directories(root / "sub");

    std::ofstream(root / "real.txt") << "hello";
    std::ofstream(root / "sub" / "nested.txt") << "world";

    std::error_code ec;
    fs::create_symlink(".", root / "loop", ec);
    ASSERT_FALSE(ec) << "Failed to create symlink `loop`: " << ec.message();
    fs::create_symlink("..", root / "sub" / "back", ec);
    ASSERT_FALSE(ec) << "Failed to create symlink `sub/back`: " << ec.message();

    auto storage = makeLocalObjectStorage(root.string());

    DB::RelativePathsWithMetadata children;
    /// Must return (not recurse forever / crash with stack overflow).
    storage->listObjects(root.string(), children, /* max_keys */ 0);

    std::vector<std::string> paths;
    paths.reserve(children.size());
    for (const auto & c : children)
        paths.push_back(c->relative_path);
    std::sort(paths.begin(), paths.end());

    /// Only the real files should be reported; the symlinks to directories
    /// are skipped (the default `recursive_directory_iterator` behaviour).
    ASSERT_EQ(paths.size(), 2u) << "Unexpected listing: {"
        << (paths.empty() ? std::string() : paths[0])
        << (paths.size() > 1 ? (std::string(", ") + paths[1]) : std::string()) << "}";
    EXPECT_EQ(paths[0], (root / "real.txt").string());
    EXPECT_EQ(paths[1], (root / "sub" / "nested.txt").string());
}

/// Sanity: a plain nested tree without symlinks should still be listed
/// exhaustively (i.e. the fix must not regress the common case).
TEST(LocalObjectStorage, ListObjectsWalksNestedDirectoriesWithoutSymlinks)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_nested");
    const auto & root = tmp.path;

    fs::create_directories(root / "a" / "b" / "c");
    std::ofstream(root / "top.txt") << "0";
    std::ofstream(root / "a" / "a_file.txt") << "1";
    std::ofstream(root / "a" / "b" / "b_file.txt") << "2";
    std::ofstream(root / "a" / "b" / "c" / "c_file.txt") << "3";

    auto storage = makeLocalObjectStorage(root.string());

    DB::RelativePathsWithMetadata children;
    storage->listObjects(root.string(), children, /* max_keys */ 0);

    std::vector<std::string> paths;
    paths.reserve(children.size());
    for (const auto & c : children)
        paths.push_back(c->relative_path);
    std::sort(paths.begin(), paths.end());

    ASSERT_EQ(paths.size(), 4u);
    EXPECT_EQ(paths[0], (root / "a" / "a_file.txt").string());
    EXPECT_EQ(paths[1], (root / "a" / "b" / "b_file.txt").string());
    EXPECT_EQ(paths[2], (root / "a" / "b" / "c" / "c_file.txt").string());
    EXPECT_EQ(paths[3], (root / "top.txt").string());
}

/// Regression test for the serverfuzz finding STID 1615-3a7b on master.
/// The `AST fuzzer` injected an embedded NUL byte into an `icebergLocal(...)`
/// path literal, turning the argument into `'<user_files>/lakehouses\0test_3_t0'`.
/// `std::string` preserves the NUL, but every syscall goes through
/// `path.c_str()` which truncates at the NUL — so the kernel always targets
/// the same real directory. With the previous self-recursive implementation
/// of `listObjects`, each recursion created a fresh `fs::directory_iterator`
/// via `::opendir(path.c_str())` on a progressively longer C++ string that
/// still resolves to the same directory — an unbounded self-recursion that
/// blew the thread stack under `ThreadFuzzer` + debug builds (~40 frames of
/// `LocalObjectStorage.cpp:292` in the crash trace).
///
/// `recursive_directory_iterator` descends via `openat(dir_fd, entry_name)`
/// rather than `opendir(full_path)`, so it does not re-open the same
/// directory at every level and its traversal is iterative with constant
/// C++ stack depth. The `listObjects` call must therefore return (either
/// producing the real directory contents, or throwing a `filesystem_error`
/// when subsequent metadata calls hit the kernel truncation) — but it must
/// never crash with a stack overflow.
TEST(LocalObjectStorage, ListObjectsHandlesPathWithEmbeddedNul)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_nul");
    const auto & root = tmp.path;

    /// Small nested tree so the iterator actually descends.
    fs::create_directories(root / "d1" / "d2");
    std::ofstream(root / "top.txt") << "0";
    std::ofstream(root / "d1" / "f1.txt") << "1";
    std::ofstream(root / "d1" / "d2" / "f2.txt") << "2";

    auto storage = makeLocalObjectStorage(root.string());

    /// Build the NUL-injected argument: `<root>\0<garbage>`.
    /// All libc syscalls will see just `<root>` (truncated at the NUL).
    std::string nul_injected = root.string();
    nul_injected.push_back('\0');
    nul_injected.append("this_part_is_never_seen_by_the_kernel");

    DB::RelativePathsWithMetadata children;
    /// Either outcome is acceptable; the key assertion is that the call
    /// returns control to this thread (no SIGSEGV from unbounded recursion):
    ///  - success, in which case the listing reports the real files, OR
    ///  - a `filesystem_error` from a subsequent syscall that trips on the
    ///    NUL-truncated path (e.g. `fs::file_size` on a directory).
    try
    {
        storage->listObjects(nul_injected, children, /* max_keys */ 0);
    }
    catch (const fs::filesystem_error &) // NOLINT(bugprone-empty-catch)
    {
        /// Acceptable — the NUL-injected path produced an invalid argument
        /// for a downstream syscall. Importantly, we reached the catch,
        /// meaning no stack overflow occurred in the iteration.
    }

    /// Sanity: the test finished (no crash / no infinite loop).
    SUCCEED();
}

/// Regression test for the Iceberg `test_format_version_upgrade_concurrent_reads`
/// flake. `listObjects` enumerated the directory and then stat-ed each entry with
/// the *throwing* `fs::last_write_time` / `fs::file_size`. When a concurrent writer
/// atomically replaced a file (`write tmp; rename tmp -> final`, exactly what the
/// Iceberg metadata-upgrade path does), the transient name could vanish between
/// enumeration and the stat, so `last_write_time` threw `no_such_file_or_directory`
/// and aborted the whole listing with a `filesystem_error` (surfaced as a query
/// failure: `SELECT ... FROM iceberg_table`). Listing is a best-effort snapshot, so
/// a concurrently-removed entry must simply be omitted, never throw.
///
/// This test reproduces the race deterministically: writer threads churn the
/// directory with atomic renames while reader threads call `listObjects` in a tight
/// loop. Before the fix this reliably throws within a few milliseconds; after it,
/// every listing succeeds.
TEST(LocalObjectStorage, ListObjectsToleratesConcurrentAtomicRename)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_race");
    const auto & root = tmp.path;

    /// A few stable files so a successful listing is non-empty.
    for (int i = 0; i < 4; ++i)
        std::ofstream(root / ("stable_" + std::to_string(i) + ".txt")) << i;

    auto storage = makeLocalObjectStorage(root.string());

    std::atomic<bool> stop{false};
    std::atomic<bool> listing_threw{false};
    std::atomic<size_t> listings_done{0};

    /// Writers: continuously replace `churn_<w>.json` via the same
    /// write-temp-then-rename dance used by the Iceberg metadata writer.
    auto writer_loop = [&](int w)
    {
        const fs::path final_path = root / ("churn_" + std::to_string(w) + ".json");
        size_t gen = 0;
        while (!stop.load(std::memory_order_relaxed))
        {
            const fs::path tmp_path = root / ("churn_" + std::to_string(w) + ".json.tmp." + std::to_string(gen++));
            { std::ofstream(tmp_path) << "{\"v\":" << gen << "}"; }
            std::error_code ec;
            fs::rename(tmp_path, final_path, ec);
            if (ec)
                fs::remove(tmp_path, ec);
        }
    };

    /// Readers: list the directory in a tight loop. A single throw fails the test.
    auto reader_loop = [&]()
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            try
            {
                DB::RelativePathsWithMetadata children;
                storage->listObjects(root.string(), children, /* max_keys */ 0);
                listings_done.fetch_add(1, std::memory_order_relaxed);
            }
            catch (...)
            {
                /// Ok: catch-all is the test's failure assertion. Record only an atomic
                /// flag (not the message) so the reader threads stay data-race-free under TSAN.
                listing_threw.store(true, std::memory_order_relaxed);
                return;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int w = 0; w < 3; ++w)
        threads.emplace_back(writer_loop, w);
    for (int r = 0; r < 4; ++r)
        threads.emplace_back(reader_loop);

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    stop.store(true, std::memory_order_relaxed);
    for (auto & t : threads)
        t.join();

    EXPECT_FALSE(listing_threw.load()) << "listObjects threw on a concurrently-renamed entry";
    EXPECT_GT(listings_done.load(), 0u) << "no listing completed";
}

/// Regression test for the clickhouse-gh[bot] review on PR #107432. The
/// concurrent-rename fix must suppress ONLY the disappearance race (an entry
/// removed mid-listing); a real I/O or permission error must still propagate so
/// callers never read a silently truncated `icebergLocal`/local-object-storage
/// listing. Here a subdirectory is made unreadable (mode 000): the iterator can
/// see the entry but recursing into it fails with EACCES, which must surface as
/// a `filesystem_error` rather than a short listing.
TEST(LocalObjectStorage, ListObjectsThrowsOnPermissionDeniedSubdirectory)
{
    /// Running as root bypasses permission bits, so EACCES would never trigger.
    if (::geteuid() == 0)
        GTEST_SKIP() << "must not run as root (permission checks are bypassed)";

    ScopedTempDir tmp("ch_gtest_local_object_storage_eacces");
    const auto & root = tmp.path;

    std::ofstream(root / "top.txt") << "0";
    const auto no_access = root / "noaccess";
    fs::create_directories(no_access);
    std::ofstream(no_access / "hidden.txt") << "1";

    /// Strip every permission bit: the `noaccess` entry stays visible from the
    /// root, but opening it to recurse fails with EACCES.
    std::error_code ec;
    fs::permissions(no_access, fs::perms::none, fs::perm_options::replace, ec);
    ASSERT_FALSE(ec) << "Failed to chmod 000 the subdirectory: " << ec.message();

    auto storage = makeLocalObjectStorage(root.string());

    DB::RelativePathsWithMetadata children;
    EXPECT_THROW(
        storage->listObjects(root.string(), children, /* max_keys */ 0),
        fs::filesystem_error);

    /// Restore permissions so ScopedTempDir can remove the tree on teardown.
    fs::permissions(no_access, fs::perms::owner_all, fs::perm_options::replace, ec);
}

/// Regression test for the follow-up clickhouse-gh[bot] review on PR #107432.
/// The iterator tolerates the disappearance class (ENOENT + ENOTDIR), but the
/// per-entry metadata stat (`tryGetObjectMetadata`) must apply the same tolerance:
/// after `is_directory` succeeds on a yielded child `<root>/d/file`, the parent
/// `<root>/d` can be concurrently replaced by a file, so `fs::last_write_time` /
/// `fs::file_size` on `<root>/d/file` return `not_a_directory` (ENOTDIR). That
/// is the same race the PR promises to omit, so it must yield an empty optional,
/// not abort the listing. A genuine error (EACCES) must still propagate.
TEST(LocalObjectStorage, TryGetObjectMetadataToleratesNonDirectoryPathComponent)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_enotdir");
    const auto & root = tmp.path;

    auto storage = makeLocalObjectStorage(root.string());

    /// `<root>/d` is a regular file, so stat-ing `<root>/d/file` walks through a
    /// non-directory component and fails with ENOTDIR (`not_a_directory`).
    std::ofstream(root / "d") << "x";
    EXPECT_NO_THROW({
        auto metadata = storage->tryGetObjectMetadata((root / "d" / "file").string(), /*with_tags=*/ false);
        EXPECT_FALSE(metadata.has_value()) << "a vanished (ENOTDIR) entry must be omitted, not reported";
    });

    /// A genuine, non-disappearance error must still propagate. A subdirectory
    /// stripped of all permissions makes the traversal fail with EACCES.
    if (::geteuid() != 0) /// root bypasses permission bits
    {
        const auto no_access = root / "noaccess";
        fs::create_directories(no_access);
        std::ofstream(no_access / "hidden.txt") << "1";

        std::error_code ec;
        fs::permissions(no_access, fs::perms::none, fs::perm_options::replace, ec);
        ASSERT_FALSE(ec) << "Failed to chmod 000 the subdirectory: " << ec.message();

        EXPECT_THROW(
            storage->tryGetObjectMetadata((no_access / "hidden.txt").string(), /*with_tags=*/ false),
            fs::filesystem_error);

        fs::permissions(no_access, fs::perms::owner_all, fs::perm_options::replace, ec);
    }
}

/// A non-existent or non-directory input must return an empty listing,
/// never throw or crash.
TEST(LocalObjectStorage, ListObjectsHandlesMissingAndNonDirectoryPaths)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_missing");
    const auto & root = tmp.path;

    auto storage = makeLocalObjectStorage(root.string());

    /// Missing path.
    {
        DB::RelativePathsWithMetadata children;
        storage->listObjects((root / "does_not_exist").string(), children, /* max_keys */ 0);
        EXPECT_TRUE(children.empty());
    }

    /// Regular file (not a directory).
    {
        const auto file_path = root / "just_a_file.txt";
        std::ofstream(file_path) << "x";
        DB::RelativePathsWithMetadata children;
        storage->listObjects(file_path.string(), children, /* max_keys */ 0);
        EXPECT_TRUE(children.empty());
    }
}
