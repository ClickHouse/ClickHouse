#include <Storages/MergeTree/MergeTreeIndexHypothesis.h>
#include <Storages/MergeTree/MergeTreeIndexHypothesisMergedCondition.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MergeTreeIndexGranuleHypothesis::MergeTreeIndexGranuleHypothesis(const String & index_name_)
    : index_name(index_name_), is_empty(true), met(false)
{
}

MergeTreeIndexGranuleHypothesis::MergeTreeIndexGranuleHypothesis(const String & index_name_, bool met_)
    : index_name(index_name_), is_empty(false), met(met_)
{
}

void MergeTreeIndexGranuleHypothesis::serializeBinary(WriteBuffer & ostr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt8>());
    size_type->getDefaultSerialization()->serializeBinary(static_cast<UInt8>(met), ostr, {});
}

void MergeTreeIndexGranuleHypothesis::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_met;
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt8>());
    size_type->getDefaultSerialization()->deserializeBinary(field_met, istr, {});
    met = field_met.safeGet<UInt8>();
    is_empty = false;
}

MergeTreeIndexAggregatorHypothesis::MergeTreeIndexAggregatorHypothesis(const String & index_name_, const String & column_name_)
    : index_name(index_name_), column_name(column_name_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorHypothesis::getGranuleAndReset()
{
    const auto granule = std::make_shared<MergeTreeIndexGranuleHypothesis>(index_name, met);
    met = true;
    is_empty = true;
    return granule;
}

void MergeTreeIndexAggregatorHypothesis::update(const Block & block, size_t * pos, size_t limit)
{
    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;
    const auto & column = block.getByName(column_name).column->cut(*pos, rows_read);

    if (!column->hasEqualValues() || column->get64(0) == 0)
        met = false;

    is_empty = false;
    *pos += rows_read;
}

MergeTreeIndexGranulePtr MergeTreeIndexHypothesis::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleHypothesis>(index.name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexHypothesis::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorHypothesis>(index.name, index.sample_block.getNames().front());
}

MergeTreeIndexConditionPtr MergeTreeIndexHypothesis::createIndexCondition(
    const ActionsDAG *, ContextPtr) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported");
}

MergeTreeIndexMergedConditionPtr MergeTreeIndexHypothesis::createIndexMergedCondition(
    const SelectQueryInfo & query_info, StorageMetadataPtr storage_metadata) const
{
    return std::make_shared<MergeTreeIndexhypothesisMergedCondition>(
        query_info, storage_metadata->getConstraints(), index.granularity);
}

MergeTreeIndexPtr hypothesisIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexHypothesis>(index);
}

void hypothesisIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    if (index.expression_list_ast->children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hypothesis index needs exactly one expression");
}

}
