#include <gtest/gtest.h>

#include <Common/filesystemHelpers.h>

#include <filesystem>
#include <string>

#include <unistd.h> /// for ::getpid

namespace fs = std::filesystem;

namespace
{

/// The containment helpers compare the whole string, while every syscall a path is later passed to stops at
/// the first NUL byte. The tests below build the shape that abuses the difference: as a whole the path
/// normalizes back into the prefix, truncated at the NUL it addresses `probe` next to the prefix.
struct EmbeddedNulPaths
{
    fs::path prefix;
    std::string contained;
    std::string escaping_through_nul;

    EmbeddedNulPaths()
        : prefix(fs::temp_directory_path() / ("path_starts_with_embedded_nul_" + std::to_string(::getpid())))
    {
        fs::create_directories(prefix);
        std::string back_into_prefix = "/../" + prefix.filename().string() + "/file";
        contained = (prefix / ".." / "probe").string() + back_into_prefix;
        escaping_through_nul = (prefix / ".." / "probe").string() + std::string(1, '\0') + back_into_prefix;
    }

    ~EmbeddedNulPaths()
    {
        std::error_code ec;
        fs::remove_all(prefix, ec);
    }
};

std::string withEmbeddedNul(const fs::path & path)
{
    return path.string() + std::string(1, '\0') + "suffix";
}

}

TEST(PathStartsWithEmbeddedNul, PathStartsWith)
{
    EmbeddedNulPaths paths;
    const std::string prefix = paths.prefix.string();

    /// Positive control: the same traversal without a NUL is contained, so it is the NUL that flips the result.
    EXPECT_TRUE(DB::pathStartsWith(paths.contained, prefix));
    EXPECT_TRUE(DB::pathStartsWith(fs::path(paths.contained), paths.prefix));
    EXPECT_TRUE(DB::pathStartsWith((paths.prefix / "file").string(), prefix));

    EXPECT_FALSE(DB::pathStartsWith(paths.escaping_through_nul, prefix));
    EXPECT_FALSE(DB::pathStartsWith(fs::path(paths.escaping_through_nul), paths.prefix));

    /// A NUL anywhere in the path, even where it cannot escape, and a NUL in the prefix are rejected too.
    EXPECT_FALSE(DB::pathStartsWith(withEmbeddedNul(paths.prefix / "file"), prefix));
    EXPECT_FALSE(DB::pathStartsWith((paths.prefix / "file").string(), withEmbeddedNul(paths.prefix)));
    EXPECT_FALSE(DB::pathStartsWith(fs::path((paths.prefix / "file").string()), fs::path(withEmbeddedNul(paths.prefix))));
}

TEST(PathStartsWithEmbeddedNul, FileOrSymlinkPathStartsWith)
{
    EmbeddedNulPaths paths;
    const std::string prefix = paths.prefix.string();

    EXPECT_TRUE(DB::fileOrSymlinkPathStartsWith(paths.contained, prefix));
    EXPECT_TRUE(DB::fileOrSymlinkPathStartsWith((paths.prefix / "file").string(), prefix));

    EXPECT_FALSE(DB::fileOrSymlinkPathStartsWith(paths.escaping_through_nul, prefix));

    EXPECT_FALSE(DB::fileOrSymlinkPathStartsWith(withEmbeddedNul(paths.prefix / "file"), prefix));
    EXPECT_FALSE(DB::fileOrSymlinkPathStartsWith((paths.prefix / "file").string(), withEmbeddedNul(paths.prefix)));
}
