#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <Processors/ISimpleTransform.h>
#include <Poco/Logger.h>
#include <Interpreters/Set.h>

namespace DB
{

struct SetWithState : public Set
{
    using Set::Set;

    /// Flow: Creating -> Finished or Suspended
    enum class State : uint8_t
    {
        /// Set is not yet created,
        /// Creating processor continues to build set.
        /// Filtering bypasses data.
        Creating,

        /// Set is finished.
        /// Creating processor is finished.
        /// Filtering filter stream with this set.
        Finished,

        /// Set building is canceled (due to limit exceeded).
        /// Creating and filtering processors bypass data.
        Suspended,
    };

    std::atomic<State> state = State::Creating;

    /// Track number of processors that are currently working on this set.
    /// Last one finalizes set.
    std::atomic_size_t finished_count = 0;
};

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
    CreatingSetsOnTheFlyTransform(const Block & header_, const Names & column_names_, size_t num_streams_, SetWithStatePtr set_);

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
    FilterBySetOnTheFlyTransform(const Block & header_, const Names & column_names_, SetWithStatePtr set_);

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
