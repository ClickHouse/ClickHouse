#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <gtest/gtest.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}
}

TEST(ColumnTuple, EmptyTupleIndex)
{
    ColumnPtr tuple_col = ColumnTuple::create(5);

    auto indexes = ColumnUInt64::create();
    auto & data = indexes->getData();
    data.push_back(0);
    data.push_back(1);
    data.push_back(2);

    ColumnPtr res_lim_zero = tuple_col->index(*indexes, 0);

    ASSERT_EQ(res_lim_zero->size(), indexes->size());

    ColumnPtr res_lim_non_zero = tuple_col->index(*indexes, 1);

    ASSERT_EQ(res_lim_non_zero->size(), 1);

    try
    {
        (void)tuple_col->index(*indexes, 10);
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}

TEST(ColumnTuple, EmptyTupleExpand)
{
    ColumnPtr tuple_col = ColumnTuple::create(5);

    IColumn::Filter mask;
    mask.push_back(1);
    mask.push_back(0);
    mask.push_back(1);
    mask.push_back(0);
    mask.push_back(1);

    ColumnPtr filtered = tuple_col->filter(mask, -1);
    ASSERT_EQ(filtered->size(), 3);

    auto expanded = IColumn::mutate(filtered);
    expanded->expand(mask, false);

    ASSERT_EQ(expanded->size(), mask.size());

    IColumn::Filter inverted_mask;
    inverted_mask.push_back(0);
    inverted_mask.push_back(1);
    inverted_mask.push_back(0);
    inverted_mask.push_back(1);
    inverted_mask.push_back(0);

    ColumnPtr filtered_inv = tuple_col->filter(inverted_mask, -1);
    ASSERT_EQ(filtered_inv->size(), 2);

    auto expanded_inv = IColumn::mutate(filtered_inv);
    expanded_inv->expand(mask, true);

    ASSERT_EQ(expanded_inv->size(), mask.size());
}

TEST(ColumnTuple, EmptyTuplePermute)
{
    ColumnPtr tuple_col = ColumnTuple::create(5);

    IColumn::Permutation perm;
    perm.push_back(0);
    perm.push_back(1);
    perm.push_back(2);

    ColumnPtr res_ok = tuple_col->permute(perm, 2);
    ASSERT_EQ(res_ok->size(), 2);

    IColumn::Permutation short_perm;
    short_perm.push_back(0);
    short_perm.push_back(1);

    try
    {
        (void)tuple_col->permute(short_perm, 3);
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}
