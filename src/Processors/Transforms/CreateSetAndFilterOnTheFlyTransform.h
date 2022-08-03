#pragma once
#include <atomic>
#include <mutex>
#include <vector>
#include <Processors/ISimpleTransform.h>
#include <Poco/Logger.h>
#include <Interpreters/Set.h>


namespace DB
{

class SetWithState : public Set
{
public:
    using Set::Set;

    enum class State
    {
        Creating,
        Finished,
        Suspended,
    };

    std::atomic<State> state = State::Creating;
    std::atomic_size_t finished_count = 0;
};

using SetWithStatePtr = std::shared_ptr<SetWithState>;

/*
 * Create a set on the fly.
 * The set is created from the key columns of the input block.
 * Data is not changed and returned as is.
 * Can be executed only in one stream.
 */
class CreatingSetsOnTheFlyTransform : public ISimpleTransform
{
public:
    explicit CreatingSetsOnTheFlyTransform(
        const Block & header_, const Names & column_names_, size_t num_streams_, SetWithStatePtr set_);

    String getName() const override { return "CreatingSetsOnTheFlyTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    Names column_names;
    std::vector<size_t> key_column_indices;

    size_t num_streams;

    /// Set to fill
    SetWithStatePtr set;

    Poco::Logger * log;
};


/*
 * Filter the input chunk by the set.
 * When set building is not completed, just return the source data.
 */
class FilterBySetOnTheFlyTransform : public ISimpleTransform
{
public:
    explicit FilterBySetOnTheFlyTransform(
        const Block & header_, const Names & column_names_, SetWithStatePtr set_);

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

    struct Stat
    {
        size_t consumed_rows = 0;
        size_t consumed_rows_before_set = 0;
        size_t result_rows = 0;
    } stat;

    Poco::Logger * log;
};



}
