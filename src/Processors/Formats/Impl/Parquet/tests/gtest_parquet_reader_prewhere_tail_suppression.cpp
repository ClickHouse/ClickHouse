#include <gtest/gtest.h>

#include <Processors/Formats/Impl/Parquet/Reader.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/SelectQueryInfo.h>

using namespace DB;
using namespace DB::Parquet;

/// `Reader::preparePrewhere` must leave no primitive column scheduled for a decode that nothing can
/// consume. A primitive whose `idx_in_output_block` is past `sample_block` occupies a slot that
/// `applyPrewhere` pops off `RowSubgroup::output` after the last PREWHERE step; if no step claimed
/// that column, the main step still decodes it and `output.at(idx_in_output_block)` throws
/// `std::out_of_range`, which the server turns into an abort. The invariant is therefore over
/// `Reader`'s own scheduling state, not over any query result: after `preparePrewhere`, a
/// tail-slot primitive that no step claimed carries the `SIZE_MAX` sentinel that keeps
/// `ReadManager` from ever scheduling it.
///
/// The state is asserted here rather than through SQL because it is not SQL-constructible: over 979
/// observations across the parquet corpus and 28 targeted query shapes, no query left a tail-slot
/// primitive at `first_step_to_calculate == 0`. The specific input that produces it in CI is
/// unidentified; a query test written today would pass on unfixed code and assert nothing.
///
/// The three assertions are one witness and two anti-over-broadness controls: a claimed PREWHERE
/// input must keep its step index, and a primitive inside `sample_block` must stay at 0. A
/// single-assertion test would also be satisfied by suppressing every column.
namespace
{

struct Fixture
{
    Block sample_block;
    ReadOptions options;
    FormatFilterInfoPtr filter_info = std::make_shared<FormatFilterInfo>();
    Reader reader;

    /// `a`, `b` are delivered columns; `pw` and `tail` sit past `sample_block`, i.e. in the tail that
    /// `applyPrewhere` drops. PREWHERE claims `pw` only.
    Fixture()
    {
        auto type = std::make_shared<DataTypeUInt64>();
        const std::vector<String> names = {"a", "b", "pw", "tail"};

        for (size_t i = 0; i < names.size(); ++i)
        {
            ColumnWithTypeAndName col(type->createColumn(), type, names[i]);
            if (i < 2)
                sample_block.insert(col);
            reader.extended_sample_block.insert(col);

            Reader::OutputColumnInfo output;
            output.name = names[i];
            output.primitive_start = i;
            output.primitive_end = i + 1;
            output.input_type = type;
            output.output_type = type;
            output.idx_in_output_block = i;
            output.is_primitive = true;
            reader.output_columns.push_back(std::move(output));

            Reader::PrimitiveColumnInfo primitive;
            primitive.name = names[i];
            primitive.idx_in_output_block = i;
            primitive.decoded_type = type;
            primitive.output_type = type;
            reader.primitive_columns.push_back(std::move(primitive));

            reader.sample_block_to_output_columns_idx.push_back(i);
        }

        NamesAndTypesList prewhere_inputs;
        prewhere_inputs.emplace_back("pw", type);
        filter_info->prewhere_info = std::make_shared<PrewhereInfo>(ActionsDAG(prewhere_inputs), "pw");

        reader.init(options, sample_block, filter_info);
    }
};

}

TEST(ParquetReaderPrewhere, SuppressesUnclaimedTailColumn)
{
    Fixture f;
    f.reader.preparePrewhere();

    ASSERT_EQ(f.reader.steps.size(), 1u);

    /// The witness: nothing claimed `tail`, and its slot is dropped after the last step, so it must
    /// never be scheduled.
    EXPECT_EQ(f.reader.primitive_columns[3].first_step_to_calculate, SIZE_MAX);

    /// `pw` is a PREWHERE input, so its step index must survive untouched.
    EXPECT_EQ(f.reader.primitive_columns[2].first_step_to_calculate, 1u);

    /// `a` and `b` are delivered, so the main step must still decode them.
    EXPECT_EQ(f.reader.primitive_columns[0].first_step_to_calculate, 0u);
    EXPECT_EQ(f.reader.primitive_columns[1].first_step_to_calculate, 0u);
}

/// `PrimitiveColumnInfo::idx_in_output_block` defaults to `UINT64_MAX`, and `SchemaConverter`
/// guarantees it was resolved only with a `chassert`, which is a no-op outside debug and sanitizer
/// builds. An unresolved index compares greater than `sample_block->columns()`, so without the upper
/// bound against `extended_sample_block` such a column would be silently dropped from the read
/// instead of tripping the assert.
TEST(ParquetReaderPrewhere, KeepsUnresolvedOutputIndexScheduled)
{
    Fixture f;

    Reader::PrimitiveColumnInfo unresolved;
    unresolved.name = "unresolved";
    unresolved.decoded_type = std::make_shared<DataTypeUInt64>();
    unresolved.output_type = unresolved.decoded_type;
    ASSERT_EQ(unresolved.idx_in_output_block, UINT64_MAX);
    f.reader.primitive_columns.push_back(std::move(unresolved));

    f.reader.preparePrewhere();

    EXPECT_EQ(f.reader.primitive_columns[4].first_step_to_calculate, 0u);
}
