#include <Storages/MergeTree/MergeTreeIndexLegacyHypothesis.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_INDEX;
}

MergeTreeIndexLegacyHypothesis::MergeTreeIndexLegacyHypothesis(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
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

MergeTreeIndexPtr legacyHypothesisIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexLegacyHypothesis>(std::move(metadata_snapshot), index);
}

void legacyHypothesisIndexValidator(const IndexDescription &, bool attach)
{
    if (!attach)
        throw Exception(ErrorCodes::ILLEGAL_INDEX, "Index of type 'hypothesis' is no longer supported. Please drop the index");
}

}
