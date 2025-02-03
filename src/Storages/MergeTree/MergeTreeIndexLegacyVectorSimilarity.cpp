#include <Storages/MergeTree/MergeTreeIndexLegacyVectorSimilarity.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_INDEX;
}

MergeTreeIndexLegacyVectorSimilarity::MergeTreeIndexLegacyVectorSimilarity(const IndexDescription & index_)
    : IMergeTreeIndex(index_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexLegacyVectorSimilarity::createIndexGranule() const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Indexes of type 'annoy' or 'usearch' are no longer supported. Please drop and recreate the index as type 'vector_similarity'");
}

MergeTreeIndexAggregatorPtr MergeTreeIndexLegacyVectorSimilarity::createIndexAggregator(const MergeTreeWriterSettings &) const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Indexes of type 'annoy' or 'usearch' are no longer supported. Please drop and recreate the index as type 'vector_similarity'");
}

MergeTreeIndexConditionPtr MergeTreeIndexLegacyVectorSimilarity::createIndexCondition(const SelectQueryInfo &, ContextPtr) const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Indexes of type 'annoy' or 'usearch' are no longer supported. Please drop and recreate the index as type 'vector_similarity'");
};

MergeTreeIndexConditionPtr MergeTreeIndexLegacyVectorSimilarity::createIndexCondition(const ActionsDAG *, ContextPtr) const
{
    throw Exception(ErrorCodes::ILLEGAL_INDEX, "Indexes of type 'annoy' or 'usearch' are no longer supported. Please drop and recreate the index as type 'vector_similarity'");
}

MergeTreeIndexPtr legacyVectorSimilarityIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexLegacyVectorSimilarity>(index);
}

void legacyVectorSimilarityIndexValidator(const IndexDescription &, bool)
{
}

}
