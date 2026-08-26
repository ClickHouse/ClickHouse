#include <gtest/gtest.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>

using namespace DB;

namespace
{
    /// A DiskLocal-backed parent part storage. `DiskLocal::createTransaction` yields a real
    /// transaction object, which is all `beginTransaction` needs to hand a NON-NULL transaction to a
    /// borrowed projection sub-part (the `has_shared_transaction == true` case).
    struct ParentStorageFixture
    {
        std::filesystem::path base_path;
        DiskPtr disk;
        VolumePtr volume;
        MutableDataPartStoragePtr parent;

        ParentStorageFixture()
        {
            const auto unique = std::to_string(::getpid()) + "_"
                + std::to_string(reinterpret_cast<uintptr_t>(this));
            base_path = std::filesystem::temp_directory_path() / ("proj_txn_gtest_" + unique);
            std::filesystem::create_directories(base_path / "all_1_1_0");
            disk = std::make_shared<DiskLocal>("test_disk_" + unique, base_path.string());
            volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
            parent = std::make_shared<DataPartStorageOnDiskFull>(volume, /*root_path=*/"", "all_1_1_0");
        }

        ~ParentStorageFixture()
        {
            std::error_code ec;
            std::filesystem::remove_all(base_path, ec);
        }
    };
}

/// A projection sub-part that BORROWS the parent's whole-part transaction (the CA-disk shape:
/// getProjection(..., use_parent_transaction = true)) must let begin/commit be NO-OPS — it rides the
/// parent's single commit. Before the encapsulation this threw "Uncommitted shared transaction already
/// exists" / "Cannot commit shared transaction", forcing every caller to branch on isContentAddressed().
TEST(ProjectionBorrowedTransaction, BorrowedStorageBeginCommitAreNoOps)
{
    ParentStorageFixture fx;

    /// Parent opens the whole-part transaction (as MergeTask/writer do for a CA part).
    fx.parent->beginTransaction();
    ASSERT_TRUE(fx.parent->hasActiveTransaction());

    /// Borrowed projection sub-part: shares the parent transaction (has_shared_transaction == true).
    auto proj = fx.parent->getProjection("p.proj", /*use_parent_transaction=*/true);
    EXPECT_TRUE(proj->hasActiveTransaction());

    /// The encapsulated rule: begin/commit on the borrowed storage are silent no-ops (they must NOT
    /// open a second transaction, nor commit the parent's).
    EXPECT_NO_THROW(proj->beginTransaction());
    EXPECT_NO_THROW(proj->commitTransaction());

    /// The parent's transaction is untouched by the projection's no-ops and still commits cleanly.
    EXPECT_TRUE(fx.parent->hasActiveTransaction());
    EXPECT_NO_THROW(fx.parent->commitTransaction());
    EXPECT_FALSE(fx.parent->hasActiveTransaction());
}

/// The non-CA temp-projection shape (use_parent_transaction = false) is unchanged: the sub-part OWNS
/// its transaction, so begin creates it and commit commits it (has_shared_transaction == false, so the
/// no-op path never triggers).
TEST(ProjectionBorrowedTransaction, OwnedProjectionStorageStillBeginsAndCommits)
{
    ParentStorageFixture fx;

    auto proj = fx.parent->getProjection("q.proj", /*use_parent_transaction=*/false);
    EXPECT_FALSE(proj->hasActiveTransaction());

    EXPECT_NO_THROW(proj->beginTransaction());
    EXPECT_TRUE(proj->hasActiveTransaction());
    EXPECT_NO_THROW(proj->commitTransaction());
    EXPECT_FALSE(proj->hasActiveTransaction());
}
