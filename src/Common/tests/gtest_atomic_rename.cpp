#include <gtest/gtest.h>

#include <Common/atomicRename.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include <unistd.h>

namespace fs = std::filesystem;

namespace
{

std::string readAll(const fs::path & path)
{
    std::ifstream in(path);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

void writeAll(const fs::path & path, const std::string & content)
{
    std::ofstream out(path);
    out << content;
}

/// Drives DB::renameExchangeNonAtomic -- the fallback used by DiskLocal::renameExchange on
/// filesystems that do not support renameat2(RENAME_EXCHANGE). Normal CI runs on filesystems
/// where the atomic exchange succeeds, so this path is otherwise never exercised.
class RenameExchangeNonAtomicTest : public ::testing::Test
{
protected:
    fs::path dir;

    void SetUp() override
    {
        const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
        dir = fs::temp_directory_path()
            / ("ch_gtest_rename_exchange_" + std::to_string(getpid()) + "_" + info->name());
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);
    }

    void TearDown() override
    {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

}

TEST_F(RenameExchangeNonAtomicTest, SwapsTwoExistingFiles)
{
    const auto a = dir / "a.sql";
    const auto b = dir / "b.sql";
    writeAll(a, "AAA");
    writeAll(b, "BBB");

    DB::renameExchangeNonAtomic(a.string(), b.string());

    EXPECT_EQ(readAll(a), "BBB");
    EXPECT_EQ(readAll(b), "AAA");
    /// No temporary file left behind.
    EXPECT_FALSE(fs::exists(a.string() + ".tmp_rename_exchange"));
}

TEST_F(RenameExchangeNonAtomicTest, MovesWhenTargetMissing)
{
    const auto a = dir / "a.sql";
    const auto b = dir / "b.sql";
    writeAll(a, "AAA");

    DB::renameExchangeNonAtomic(a.string(), b.string());

    EXPECT_FALSE(fs::exists(a));
    ASSERT_TRUE(fs::exists(b));
    EXPECT_EQ(readAll(b), "AAA");
}

/// The `Atomic` database reaches its metadata `.sql` files through a directory symlink
/// (metadata/<db> -> store/<uuid>). Make sure the fallback swaps correctly through it.
TEST_F(RenameExchangeNonAtomicTest, WorksThroughDirectorySymlink)
{
    fs::create_directories(dir / "store");
    fs::create_directory_symlink("store", dir / "meta");

    const auto a = dir / "meta" / "A.sql";
    const auto b = dir / "meta" / "B.sql";
    writeAll(a, "TABLE_A");
    writeAll(b, "TABLE_B");

    DB::renameExchangeNonAtomic(a.string(), b.string());

    EXPECT_EQ(readAll(a), "TABLE_B");
    EXPECT_EQ(readAll(b), "TABLE_A");
    /// The physical files under the symlink target are swapped too.
    EXPECT_EQ(readAll(dir / "store" / "A.sql"), "TABLE_B");
    EXPECT_EQ(readAll(dir / "store" / "B.sql"), "TABLE_A");
}
