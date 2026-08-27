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

    /// `acc.found` tracks whether a usable point was ever added, not whether any rows were seen --
    /// a granule made up only of empty geometries (or only non-finite ones) leaves `acc.found == false`
    /// even though rows were processed. `empty()` must reflect "no rows seen" so the writer still flushes
    /// a (non-prunable) granule placeholder for such rows instead of dropping them from the index entirely.
    bool empty() const override { return rows_seen == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    String index_name;
    String column_name;
    BboxAccumulator acc;
    size_t rows_seen = 0;
};


/// Evaluates whether a granule can be skipped for a spatial query predicate.
class MergeTreeIndexConditionSpatialBbox final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionSpatialBbox(
        const String & column_name_,
        const ActionsDAG::Node * predicate,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override { return query_bboxes.empty(); }

    bool mayBeTrueOnGranule(
        MergeTreeIndexGranulePtr idx_granule,
        const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;

    std::string getDescription() const override;

private:
    /// Find every spatial predicate in the DAG node tree that filters the indexed column using a
    /// constant geometry, and return one bounding box PER conjunct. Shares its node-level
    /// extraction and conjunction-walking logic with `Parquet` row-group pruning via
    /// `extractSpatialPredicateNodeBbox`/`collectConjunctiveSpatialBboxes` (see `Common/GeoBbox.h`),
    /// which keeps the boxes separate in the same way.
    static std::vector<QueryBbox> extractQueryBboxes(
        const ActionsDAG::Node * node,
        const String & col_name);

    String column_name;

    /// The conjuncts are kept apart on purpose and are NOT folded into one box. A matching row's
    /// geometry bbox must intersect each of them individually, which does not imply that it
    /// intersects their intersection: one polygon can contain two far-apart points, satisfying both
    /// `pointInPolygon((0, 0), poly)` and `pointInPolygon((10, 10), poly)`, while those two
    /// zero-area query boxes do not overlap at all. Intersecting them up front would prune every
    /// granule for such a query -- see
    /// `05050_spatial_bbox_conjunct_bboxes_not_intersected`.
    std::vector<QueryBbox> query_bboxes;
};


class MergeTreeIndexSpatialBbox final : public IMergeTreeIndex
{
public:
    MergeTreeIndexSpatialBbox(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const override;

    MergeTreeIndexSubstreams getSubstreams() const override
    {
        return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}};
    }

    /// This index has only ever been written as `.idx2`, so -- unlike the base implementation --
    /// there is no legacy `.idx` layout to discover. The read-time usability checks (invalidated
    /// system columns, part/metadata type compatibility) stay in the non-virtual
    /// `IMergeTreeIndex::getDeserializedFormat`, which calls this.
    using IMergeTreeIndex::getPhysicalFormat;
    MergeTreeIndexFormat getPhysicalFormat(
        const MergeTreeDataPartChecksums & checksums,
        const IDataPartStorage & storage,
        const std::string & relative_path_prefix) const override;
};

}
