#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <unistd.h> /// for ::getpid

#include <algorithm> /// for std::sort
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
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

/// Regression test for the fifth clickhouse-gh[bot] review on PR #107432.
/// The explicit-stack traversal can loop forever on an embedded-NUL root when
/// the real directory contains ONLY subdirectories (no regular file to trip the
/// per-entry stat). Trace: `path = <root>\0tail`, `<root>` contains only `d1/`.
/// `directory_iterator(path)` opens `<root>` (truncated at the NUL), yields
/// `<root>\0tail/d1`; that child path is queued; the next
/// `directory_iterator(<root>\0tail/d1)` is truncated by libc back to `<root>`,
/// re-yields `d1`, queues `<root>\0tail/d1/d1`, ... unbounded. The earlier
/// `ListObjectsHandlesPathWithEmbeddedNul` test escapes only because its
/// top-level `top.txt` reaches `tryGetObjectMetadata`, whose truncated stat
/// targets the `<root>` directory and throws before the phantom directory is
/// drained. A directory-only root never reaches that stat, so the loop never
/// terminates. The fix rejects embedded-NUL input up front, so the call throws
/// immediately regardless of the directory contents. The listing runs on a
/// worker thread guarded by a deadline so a regression manifests as a fast FAIL
/// rather than hanging the whole test binary.
TEST(LocalObjectStorage, ListObjectsRejectsEmbeddedNulRootWithOnlySubdirectories)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_nul_dirs_only");
    const auto & root = tmp.path;

    /// Only subdirectories, NO regular file at any level: nothing trips the
    /// per-entry metadata stat, so the pre-fix loop has no escape hatch.
    fs::create_directories(root / "d1" / "d2");

    auto storage = makeLocalObjectStorage(root.string());

    /// `<root>\0tail` - every syscall sees just `<root>` (truncated at the NUL).
    std::string nul_injected = root.string();
    nul_injected.push_back('\0');
    nul_injected.append("this_part_is_never_seen_by_the_kernel");

    auto state = std::make_shared<std::mutex>();
    auto cv = std::make_shared<std::condition_variable>();
    auto done = std::make_shared<bool>(false);
    auto threw_filesystem_error = std::make_shared<bool>(false);

    /// Detached so a (regressed) infinite loop cannot wedge `join()`.
    std::thread([storage, nul_injected, state, cv, done, threw_filesystem_error]()
    {
        bool local_threw = false;
        try
        {
            DB::RelativePathsWithMetadata children;
            storage->listObjects(nul_injected, children, /* max_keys */ 0);
        }
        catch (const fs::filesystem_error &)
        {
            local_threw = true; /// expected: embedded NUL rejected up front
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Ok: any other exception type leaves `local_threw` false, so the
            /// EXPECT below fails informatively; swallowing here only keeps an
            /// unexpected type from terminating the detached thread.
        }
        {
            std::lock_guard lock(*state);
            *threw_filesystem_error = local_threw;
            *done = true;
        }
        cv->notify_one();
    }).detach();

    {
        std::unique_lock lock(*state);
        const bool finished = cv->wait_for(lock, std::chrono::seconds(20), [&] { return *done; });
        ASSERT_TRUE(finished) << "listObjects did not return on an embedded-NUL directory-only root "
                                 "(unbounded traversal loop regressed)";
        EXPECT_TRUE(*threw_filesystem_error)
            << "embedded-NUL input must be rejected with a filesystem_error";
    }
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

/// Regression test for the third clickhouse-gh[bot] review on PR #107432.
/// Tolerating the disappearance class is not enough: with a single
/// `recursive_directory_iterator`, a vanished *directory* aborts the entire
/// traversal. libc++ opens each child directory with `opendir` while
/// incrementing (`__try_recursion`); if that `opendir` fails (the directory was
/// concurrently removed), the iterator resets itself to `end()`, so every later,
/// still-present sibling is silently dropped. A caller such as
/// `DataLakeCommon::listFiles` then reads a truncated listing with no error.
///
/// The fix descends with an explicit worklist of non-recursive
/// `directory_iterator`s, so a vanished directory skips only its own subtree.
/// This test churns several subdirectories (create inner file, then remove the
/// whole directory) next to a fixed set of stable files, and asserts that every
/// successful listing still reports ALL stable files. Before the fix the listing
/// is intermittently truncated (a stable file goes missing); after it, the stable
/// set is always complete.
TEST(LocalObjectStorage, ListObjectsKeepsSiblingsWhenADirectoryVanishesMidTraversal)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_vanish_dir");
    const auto & root = tmp.path;

    /// A spread of stable files. Some will be enumerated after a churning
    /// directory, so a truncating listing would lose at least one of them.
    constexpr int stable_count = 24;
    for (int i = 0; i < stable_count; ++i)
        std::ofstream(root / ("stable_" + std::to_string(i) + ".txt")) << i;

    auto storage = makeLocalObjectStorage(root.string());

    std::atomic<bool> stop{false};
    std::atomic<bool> listing_threw{false};
    std::atomic<bool> listing_truncated{false};
    std::atomic<size_t> listings_done{0};

    /// Writers: repeatedly create a subdirectory with a file inside and then
    /// remove the whole subtree, reproducing a directory that exists during
    /// enumeration but vanishes when the lister tries to descend into it.
    auto writer_loop = [&](int w)
    {
        const fs::path dir = root / ("churn_dir_" + std::to_string(w));
        while (!stop.load(std::memory_order_relaxed))
        {
            std::error_code ec;
            fs::create_directory(dir, ec);
            std::ofstream(dir / "inner.txt") << w;
            fs::remove_all(dir, ec);
        }
    };

    /// Readers: list the directory in a tight loop. A throw, or a listing that is
    /// missing any stable file, fails the test.
    auto reader_loop = [&]()
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            DB::RelativePathsWithMetadata children;
            try
            {
                storage->listObjects(root.string(), children, /* max_keys */ 0);
            }
            catch (...)
            {
                /// Ok: catch-all is the test's failure assertion. Record only an atomic
                /// flag (not the message) so the reader threads stay data-race-free under TSAN.
                listing_threw.store(true, std::memory_order_relaxed);
                return;
            }

            size_t stable_seen = 0;
            for (const auto & c : children)
                if (fs::path(c->relative_path).filename().string().starts_with("stable_"))
                    ++stable_seen;

            if (stable_seen != static_cast<size_t>(stable_count))
            {
                listing_truncated.store(true, std::memory_order_relaxed);
                return;
            }
            listings_done.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (int w = 0; w < 4; ++w)
        threads.emplace_back(writer_loop, w);
    for (int r = 0; r < 4; ++r)
        threads.emplace_back(reader_loop);

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    stop.store(true, std::memory_order_relaxed);
    for (auto & t : threads)
        t.join();

    EXPECT_FALSE(listing_threw.load()) << "listObjects threw on a concurrently-removed directory";
    EXPECT_FALSE(listing_truncated.load())
        << "listObjects dropped stable siblings when a directory vanished mid-traversal";
    EXPECT_GT(listings_done.load(), 0u) << "no listing completed";
}

/// Regression test for the fourth clickhouse-gh[bot] review on PR #107432.
/// Each directory-typed entry is probed with `is_symlink(sym_ec)` so symlinked
/// directories are skipped (no-follow, avoids cycles) while real subdirectories
/// are descended. That probe is the fourth stat in the listing path, so its
/// error_code is routed through the same disappearance filter as iterator
/// construction / `is_directory` / `increment`: a real error (EACCES, EIO)
/// propagates, a vanished entry (ENOENT/ENOTDIR) is skipped. A non-disappearance
/// error silently dropped here would truncate the listing without an exception.
///
/// A deterministic EACCES-at-`is_symlink` case is not portably constructible: on
/// filesystems that report `d_type` in `readdir` (ext4/xfs/tmpfs, i.e. the CI
/// runners) libc++'s `directory_iterator` caches the entry type, so
/// `directory_entry::is_symlink` answers from cache with no syscall and `sym_ec`
/// can only be clear or ENOENT, never EACCES/EIO. The error routing is therefore
/// correct by construction and uniform with the other three stat sites; what is
/// testable here is the descend-vs-skip decision the probe drives. This test pins
/// that decision: a real subdirectory is descended (its file is listed) while a
/// symlink-to-directory is neither descended nor reported as an object.
TEST(LocalObjectStorage, ListObjectsDescendsRealDirsButSkipsSymlinkedDirs)
{
    ScopedTempDir tmp("ch_gtest_local_object_storage_symlink_dir");
    const auto & root = tmp.path;

    /// root/
    ///   realdir/inside.txt    <- real subdir: descended, file listed
    ///   target/leaf.txt       <- real subdir (the symlink target)
    ///   linkdir -> target     <- symlink-to-dir: not descended, not reported
    fs::create_directories(root / "realdir");
    fs::create_directories(root / "target");
    std::ofstream(root / "realdir" / "inside.txt") << "a";
    std::ofstream(root / "target" / "leaf.txt") << "b";

    std::error_code ec;
    fs::create_directory_symlink("target", root / "linkdir", ec);
    ASSERT_FALSE(ec) << "Failed to create directory symlink: " << ec.message();

    auto storage = makeLocalObjectStorage(root.string());

    DB::RelativePathsWithMetadata children;
    storage->listObjects(root.string(), children, /* max_keys */ 0);

    std::vector<std::string> paths;
    paths.reserve(children.size());
    for (const auto & c : children)
        paths.push_back(c->relative_path);
    std::sort(paths.begin(), paths.end());

    /// Only the two real files via the real directories: `linkdir` is a
    /// symlink-to-dir, so it is neither descended (no `linkdir/leaf.txt`) nor
    /// reported as an object itself.
    ASSERT_EQ(paths.size(), 2u) << "Unexpected listing size " << paths.size();
    EXPECT_EQ(paths[0], (root / "realdir" / "inside.txt").string());
    EXPECT_EQ(paths[1], (root / "target" / "leaf.txt").string());
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
