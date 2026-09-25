#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// A patch writes distinct values into distinct rows, which a Const column cannot represent.
/// Both patch entry points must refuse it rather than inherit the `IColumnHelper` implementations:
/// the copying one would return a `ColumnConst` still holding the old value, silently discarding
/// every patched value while advancing the destination versions as if it had written them.
TEST(ColumnConst, RefusesPatchApplication)
{
    auto nested = ColumnUInt64::create();
    nested->insert(1u);
    auto column_const = ColumnConst::create(std::move(nested), 3);

    auto source_column = ColumnUInt64::create();
    source_column->insert(42u);

    IColumn::Versions source_versions{2};
    IColumn::Versions destination_versions{1, 1, 1};
    IColumn::Offsets source_rows{0};
    IColumn::Offsets destination_rows{1};

    IColumn::Patch patch
    {
        .sources = {{*source_column, source_versions}},
        .src_col_indices = nullptr,
        .src_row_indices = source_rows,
        .dst_row_indices = destination_rows,
        .dst_versions = destination_versions,
    };

    auto expect_not_implemented = [](auto && call)
    {
        try
        {
            call();
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED) << e.displayText();
            return;
        }
        FAIL() << "expected an exception, but the patch was accepted";
    };

    /// Call through `IColumn`: `IColumnHelper` declares both overrides private, so going through
    /// the concrete type would not even compile without the overrides under test.
    const IColumn & constant_column = *column_const;
    expect_not_implemented([&] { constant_column.updateFrom(patch); });

    auto mutable_const = IColumn::mutate(std::move(column_const));
    IColumn & mutable_constant_column = *mutable_const;
    expect_not_implemented([&] { mutable_constant_column.updateInplaceFrom(patch); });

    /// The destination versions must be untouched: refusing must not look like a partial apply.
    EXPECT_EQ(destination_versions[1], 1u);
}
