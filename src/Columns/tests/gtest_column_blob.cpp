#include <gtest/gtest.h>

#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>

using namespace DB;

namespace
{

/// The real task deserializes the BLOB; handing back `inner` exercises the same unwrapping plumbing.
ColumnPtr makeBLOB(const ColumnPtr & inner)
{
    auto blob = ColumnBLOB::create(inner->cloneEmpty());
    blob->setFromBLOBTask([inner](const ColumnBLOB::BLOB &) { return inner; });
    return blob;
}

}

/// `ColumnBLOB` is a transport representation that must be stripped by the generic wrapper-removal
/// helpers: their callers go on to `assert_cast` the column to the one the declared type produces.
TEST(ColumnBLOB, ConvertToFullIfNeededRemovesBLOB)
{
    auto inner = ColumnUInt64::create();
    inner->insertValue(42);
    ColumnPtr inner_ptr = std::move(inner);

    auto full = makeBLOB(inner_ptr)->convertToFullIfNeeded();

    EXPECT_EQ(typeid_cast<const ColumnBLOB *>(full.get()), nullptr);
    EXPECT_EQ(full.get(), inner_ptr.get());
}

TEST(ColumnBLOB, RemoveSpecialRepresentationsRemovesBLOB)
{
    auto inner = ColumnUInt64::create();
    inner->insertValue(42);
    ColumnPtr inner_ptr = std::move(inner);

    auto full = removeSpecialRepresentations(makeBLOB(inner_ptr));

    EXPECT_EQ(typeid_cast<const ColumnBLOB *>(full.get()), nullptr);
    EXPECT_EQ(full.get(), inner_ptr.get());
}

/// A BLOB can wrap a sparse column (kind stack `{Default, Sparse, Detached}`), so removing it has to
/// leave the column it uncovers to the rest of the chain.
TEST(ColumnBLOB, RemovalContinuesIntoTheUncoveredColumn)
{
    auto values = ColumnUInt64::create();
    ColumnPtr sparse = ColumnSparse::create(std::move(values));

    auto full = removeSpecialRepresentations(makeBLOB(sparse));

    EXPECT_EQ(typeid_cast<const ColumnBLOB *>(full.get()), nullptr);
    EXPECT_EQ(typeid_cast<const ColumnSparse *>(full.get()), nullptr);
}
