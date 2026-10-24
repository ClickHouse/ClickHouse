#include <gtest/gtest.h>

#include <Common/filesystemHelpers.h>

#include <filesystem>
#include <string>

#include <unistd.h> /// for ::getpid

namespace fs = std::filesystem;

namespace
{

/// `real` is the containment root and `link` a symlink to it, as `user_files_path` can be. `real/escw` is a
/// symlink that leaves the root: following it is allowed, stepping out through it with a `..` is not.
struct Tree
{
    fs::path base;
    fs::path real;
    fs::path link;

    Tree()
        : base(fs::temp_directory_path() / ("path_starts_with_resolving_dot_dot_" + std::to_string(::getpid())))
        , real(base / "real")
        , link(base / "link")
    {
        fs::remove_all(base);
        fs::create_directories(real / "sub");
        fs::create_directories(base / "outside" / "inner");
        fs::create_directory_symlink(base / "outside" / "inner", real / "escw");
        fs::create_directory_symlink(real, link);
    }

    ~Tree()
    {
        std::error_code ec;
        fs::remove_all(base, ec);
    }
};

bool isInside(const std::string & path, const std::string & prefix)
{
    std::error_code ec;
    const bool result = DB::fileOrSymlinkPathStartsWithResolvingDotDot(path, prefix, ec);
    EXPECT_FALSE(ec) << path << ": " << ec.message();
    return result;
}

}

TEST(FileOrSymlinkPathStartsWithResolvingDotDot, Containment)
{
    Tree tree;

    for (const auto & root : {tree.real, tree.link})
    {
        SCOPED_TRACE(root);
        const std::string prefix = root.string() + "/";

        EXPECT_TRUE(isInside(root / "data.tsv", prefix));
        EXPECT_TRUE(isInside(root / "sub" / ".." / "data.tsv", prefix));
        EXPECT_TRUE(isInside(root / "escw" / "via_symlink.tsv", prefix));
        /// Folds lexically to `base/real`, which is outside `link` as written but is the same directory.
        EXPECT_TRUE(isInside(root / "sub" / ".." / ".." / "real" / "data.tsv", prefix));

        EXPECT_FALSE(isInside(root / "escw" / ".." / "secret.tsv", prefix));
        EXPECT_FALSE(isInside(root / "escw" / ".." / "inner" / ".." / "secret.tsv", prefix));
        EXPECT_FALSE(isInside(root / ".." / "outside" / "secret.tsv", prefix));
    }
}

TEST(FileOrSymlinkPathStartsWithResolvingDotDot, EmbeddedNul)
{
    Tree tree;
    const std::string prefix = tree.real.string();

    EXPECT_FALSE(isInside((tree.real / "data.tsv").string() + std::string(1, '\0'), prefix));
    EXPECT_FALSE(isInside((tree.real / "sub" / "..").string() + std::string(1, '\0') + "/data.tsv", prefix));
}
