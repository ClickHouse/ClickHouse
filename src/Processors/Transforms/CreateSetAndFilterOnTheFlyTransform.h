#pragma once

#include <vector>
#include <Processors/ISimpleTransform.h>

namespace DB
{

struct SetWithState;
using SetWithStatePtr = std::shared_ptr<SetWithState>;

/*
 * Create a set on the fly for incoming stream.
 * The set is created from the key columns of the input block.
 * Data is not changed and returned as is.
 * Can be executed in parallel, but blocks on operations with set.
 */
class CreatingSetsOnTheFlyTransform : public ISimpleTransform
{
public:
    CreatingSetsOnTheFlyTransform(SharedHeader header_, const Names & column_names_, size_t num_streams_, SetWithStatePtr set_);

    String getName() const override { return "CreatingSetsOnTheFlyTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    Names column_names;
    std::vector<size_t> key_column_indices;

    size_t num_streams;

    /// Set to fill
    SetWithStatePtr set;

    LoggerPtr log = getLogger("CreatingSetsOnTheFlyTransform");
};

/*
 * Filter the input chunk by the set.
 * When set building is not completed, just return the source data.
 */
class FilterBySetOnTheFlyTransform : public ISimpleTransform
{
public:
    FilterBySetOnTheFlyTransform(SharedHeader header_, const Names & column_names_, SetWithStatePtr set_);

    String getName() const override { return "FilterBySetOnTheFlyTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    /// Set::execute requires ColumnsWithTypesAndNames, so we need to convert Chunk to that format
    Block key_sample_block;

    Names column_names;
    std::vector<size_t> key_column_indices;

    /// Filter by this set when it's created
    SetWithStatePtr set;

    /// Statistics to log
    struct Stat
    {
        /// Total number of rows
        size_t consumed_rows = 0;

        /// Number of bypassed rows (processed before set is created)
        size_t consumed_rows_before_set = 0;

        /// Number of rows that passed the filter
        size_t result_rows = 0;
    } stat;

    LoggerPtr log = getLogger("FilterBySetOnTheFlyTransform");
};

}
