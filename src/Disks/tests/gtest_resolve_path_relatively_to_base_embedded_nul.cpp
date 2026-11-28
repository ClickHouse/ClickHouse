#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <filesystem>
#include <string>

#include <unistd.h> /// for ::getpid

namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}

namespace
{

struct ScopedBaseDir
{
    fs::path path;

    ScopedBaseDir()
        : path(fs::temp_directory_path() / ("resolve_path_relatively_to_base_embedded_nul_" + std::to_string(::getpid())))
    {
        fs::create_directories(path);
    }

    ~ScopedBaseDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

int errorCodeOf(const std::string & path, const std::string & base)
{
    try
    {
        DB::resolvePathRelativelyToBase(path, base);
    }
    catch (const DB::Exception & e)
    {
        return e.code();
    }
    return 0;
}

}

TEST(ResolvePathRelativelyToBaseEmbeddedNul, RejectsEmbeddedNul)
{
    ScopedBaseDir base;
    const std::string base_path = base.path.string();
    const std::string back_into_base = "/../" + base.path.filename().string() + "/file";

    /// Positive controls: the resolver still accepts a relative path and an absolute traversal that ends up inside.
    EXPECT_EQ(DB::resolvePathRelativelyToBase("file", base_path), (base.path / "file").string());
    const std::string contained = (base.path / ".." / "probe").string() + back_into_base;
    EXPECT_EQ(DB::resolvePathRelativelyToBase(contained, base_path), contained);

    /// Negative control: a plain traversal outside is denied by the containment check.
    EXPECT_EQ(errorCodeOf((base.path / ".." / "probe").string(), base_path), DB::ErrorCodes::PATH_ACCESS_DENIED);

    /// As a whole string, this path normalizes into the base directory; truncated at the NUL, as every syscall
    /// would see it, it addresses `probe` next to it. It must be rejected before any containment comparison.
    const std::string escaping_through_nul = (base.path / ".." / "probe").string() + std::string(1, '\0') + back_into_base;
    EXPECT_EQ(errorCodeOf(escaping_through_nul, base_path), DB::ErrorCodes::BAD_ARGUMENTS);

    /// A NUL that cannot escape is rejected all the same, in a relative and in an absolute path.
    EXPECT_EQ(errorCodeOf(std::string("file\0suffix", 11), base_path), DB::ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(errorCodeOf((base.path / "file").string() + std::string(1, '\0') + "suffix", base_path), DB::ErrorCodes::BAD_ARGUMENTS);
}
