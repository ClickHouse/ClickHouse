#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

#include <faiss/IndexIVFFlat.h>
#include <faiss/MetricType.h>
#include "Core/Field.h"
#include "Interpreters/Context_fwd.h"
#include "Parsers/IAST_fwd.h"
#include "Storages/MergeTree/CommonANNIndexes.h"
#include "Storages/SelectQueryInfo.h"
#include "base/types.h"

namespace DB
{

struct MergeTreeIndexGranuleFaiss final : public IMergeTreeIndexGranule
{
    using FaissBaseIndex = faiss::Index;
    using FaissBaseIndexPtr = std::unique_ptr<FaissBaseIndex>; 

    MergeTreeIndexGranuleFaiss(const String & index_name_, const Block & index_sample_block_);
    MergeTreeIndexGranuleFaiss(
        const String & index_name_, 
        const Block & index_sample_block_,
        FaissBaseIndexPtr index_base_,
        bool is_incomplete_);

    ~MergeTreeIndexGranuleFaiss() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;

    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override;

    String index_name;
    Block index_sample_block;
    FaissBaseIndexPtr index_base;

    // True if we tried to train index_base but we failed because of the lack of elements in the granule
    bool is_incomplete;
};


struct MergeTreeIndexAggregatorFaiss final : IMergeTreeIndexAggregator
{
    using Value = Float32;

    MergeTreeIndexAggregatorFaiss(const String & index_name_, const Block & index_sample_block_, const String & index_key_, const String & metric_type_);
    ~MergeTreeIndexAggregatorFaiss() override = default;

    bool empty() const override;
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    Block index_sample_block;
    String index_key;
    String metric_type;
    std::vector<Value> values;
    size_t dimension = 0;
};


class MergeTreeIndexConditionFaiss final : public IMergeTreeIndexConditionAnn
{
public:
    MergeTreeIndexConditionFaiss(
        const IndexDescription & index,
        const SelectQueryInfo & query,
        ContextPtr context,
        const String & metric_type_);
    ~MergeTreeIndexConditionFaiss() override = default;

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const override;

private:
    DataTypes index_data_types;
    ANNCondition::ANNCondition condition;
    String metric_type;
};


class MergeTreeIndexFaiss : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexFaiss(const IndexDescription & index_);

    ~MergeTreeIndexFaiss() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    const char* getSerializedFileExtension() const override { return ".idx2"; }
    MergeTreeIndexFormat getDeserializedFormat(const DiskPtr disk, const std::string & path_prefix) const override;

private:
    String index_key;
    String metric_type;
};

}
