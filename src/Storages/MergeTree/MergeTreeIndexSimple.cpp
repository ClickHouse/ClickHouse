#include <Storages/MergeTree/MergeTreeIndexSimple.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleSimple::MergeTreeIndexGranuleSimple()
{
    std::random_device device;
    std::mt19937 generator(device());
    std::uniform_int_distribution<> distribution;

    is_useful_random = distribution(generator) % 2;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSimple::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleSimple>();
}

void MergeTreeIndexAggregatorSimple::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);
    *pos += rows_read;
}

bool MergeTreeIndexConditionSimple::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleSimple> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleSimple>(idx_granule);
    if (!granule)
        throw Exception(
            "Simple index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    return granule->is_useful_random; 
}


MergeTreeIndexGranulePtr MergeTreeIndexSimple::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSimple>();
}


MergeTreeIndexAggregatorPtr MergeTreeIndexSimple::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSimple>();
}

MergeTreeIndexConditionPtr MergeTreeIndexSimple::createIndexCondition(
    const SelectQueryInfo & /*query*/, ContextPtr /*context*/) const
{
    return std::make_shared<MergeTreeIndexConditionSimple>();
};

MergeTreeIndexFormat MergeTreeIndexSimple::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr simpleIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexSimple>(index);
}

void simpleIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
