#pragma once
#include <vector>
#include <Processors/ISimpleTransform.h>
#include <Poco/Logger.h>


namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

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
        const Block & header_, const Names & column_names_, SetPtr set_);

    String getName() const override { return "CreatingSetsOnTheFlyTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    std::vector<size_t> key_column_indices;

    /// Set to fill
    SetPtr set;

    Poco::Logger * log;
};


/*
 * Filter the input chunk by the set.
 * When set building is not comleted, just return the source data.
 */
class FilterBySetOnTheFlyTransform : public ISimpleTransform
{
public:
    explicit FilterBySetOnTheFlyTransform(
        const Block & header_, const Names & column_names_, SetPtr set_);

    String getName() const override { return "FilterBySetOnTheFlyTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    /// Set::execute requires ColumnsWithTypesAndNames, so we need to convert Chunk to that format
    Block key_sample_block;

    std::vector<size_t> key_column_indices;

    /// Filter by this set when it's created
    SetPtr set;

    Poco::Logger * log;
};



}
