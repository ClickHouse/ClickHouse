#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <unistd.h> /// for ::getpid

#include <algorithm> /// for std::sort
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
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
