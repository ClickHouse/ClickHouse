#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

#include <Common/GeoBbox.h>
#include <optional>

namespace DB
{

/// Per-granule bounding box stored by the `spatial_bbox` skip index.
struct MergeTreeIndexGranuleSpatialBbox final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleSpatialBbox(const String & index_name_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_data; }
    size_t memoryUsageBytes() const override { return sizeof(*this); }

    String index_name;
    double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
    bool has_data = false;
};


/// Builds a `MergeTreeIndexGranuleSpatialBbox` from a block of rows.
struct MergeTreeIndexAggregatorSpatialBbox final : public IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorSpatialBbox(const String & index_name_, const String & column_name_);

    bool empty() const override { return !acc.found; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    String column_name;
    BboxAccumulator acc;
};


/// Evaluates whether a granule can be skipped for a spatial query predicate.
class MergeTreeIndexConditionSpatialBbox final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSpatialBbox(
        const String & column_name_,
        const ActionsDAG::Node * predicate,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override { return !query_bbox.has_value(); }

    bool mayBeTrueOnGranule(
        MergeTreeIndexGranulePtr idx_granule,
        const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;

    std::string getDescription() const override;

private:
    struct QueryBbox
    {
        double xmin, ymin, xmax, ymax;
    };

    /// Try to find a spatial predicate in the DAG node tree that filters the indexed column
    /// using a constant geometry. Returns the query bounding box on success.
    std::optional<QueryBbox> extractQueryBbox(
        const ActionsDAG::Node * node,
        const String & col_name);

    String column_name;
    std::optional<QueryBbox> query_bbox;
};


class MergeTreeIndexSpatialBbox final : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexSpatialBbox(const IndexDescription & index_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexSubstreams getSubstreams() const override
    {
        return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}};
    }

    MergeTreeIndexFormat getDeserializedFormat(
        const MergeTreeDataPartChecksums & checksums,
        const std::string & path_prefix) const override;
};


MergeTreeIndexPtr spatialBboxIndexCreator(const IndexDescription & index);
void spatialBboxIndexValidator(const IndexDescription & index, bool attach);

}
