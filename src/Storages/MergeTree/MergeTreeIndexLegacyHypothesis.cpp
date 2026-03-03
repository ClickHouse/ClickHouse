#include <Storages/MergeTree/MergeTreeIndexLegacyHypothesis.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_INDEX;
}

MergeTreeIndexLegacyHypothesis::MergeTreeIndexLegacyHypothesis(const IndexDescription & index_)
    : IMergeTreeIndex(index_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexLegacyHypothesis::createIndexGranule() const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Index of type 'hypothesis' is no longer supported. Please drop the index");
}

MergeTreeIndexAggregatorPtr MergeTreeIndexLegacyHypothesis::createIndexAggregator() const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Index of type 'hypothesis' is no longer supported. Please drop the index");
}

MergeTreeIndexConditionPtr MergeTreeIndexLegacyHypothesis::createIndexCondition(const ActionsDAG::Node *, ContextPtr) const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Index of type 'hypothesis' is no longer supported. Please drop the index");
}

MergeTreeIndexPtr legacyHypothesisIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexLegacyHypothesis>(index);
}

void legacyHypothesisIndexValidator(const IndexDescription &, bool attach)
{
    if (!attach)
        throw Exception(ErrorCodes::ILLEGAL_INDEX, "Index of type 'hypothesis' is no longer supported. Please drop the index");
}

}
