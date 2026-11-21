#include <gtest/gtest.h>

#include <Common/filesystemHelpers.h>

#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h> /// for ::getpid

namespace fs = std::filesystem;

#if defined(OS_LINUX)

/// A mount point with a space or a backslash is escaped in the mount table (`\040`, `\134`). The child
/// mount below must be matched through its decoded path rather than skipped, or the parent's type wins.
TEST(DirectoryFilesystemType, EscapedChildMountOverDifferentlyTypedParent)
{
    const fs::path root = fs::canonical(fs::temp_directory_path())
        / ("directory_filesystem_type_" + std::to_string(::getpid()));
    fs::create_directories(root / "data disk" / "sub");
    fs::create_directories(root / "back\\slash");
    fs::create_directories(root / "plain");

    const fs::path mounts_path = root / "mounts";
    {
        std::ofstream mounts(mounts_path);
        mounts << "rootfs / xfs rw 0 0\n"
               << "/dev/a " << root.string() << "/data\\040disk ext4 rw 0 0\n"
               << "/dev/b " << root.string() << "/back\\134slash overlay rw 0 0\n";
    }

    EXPECT_EQ(DB::getDirectoryFilesystemType((root / "data disk").string(), mounts_path.string()), "ext4");
    EXPECT_EQ(DB::getDirectoryFilesystemType((root / "data disk" / "sub").string(), mounts_path.string()), "ext4");
    EXPECT_EQ(DB::getDirectoryFilesystemType((root / "back\\slash").string(), mounts_path.string()), "overlay");
    EXPECT_EQ(DB::getDirectoryFilesystemType((root / "plain").string(), mounts_path.string()), "xfs");

    fs::remove_all(root);
}

#endif
