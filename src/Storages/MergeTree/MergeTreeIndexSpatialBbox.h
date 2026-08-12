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

    bool alwaysUnknownOrTrue() const override { return !query_bbox.has_value(); }

    bool mayBeTrueOnGranule(
        MergeTreeIndexGranulePtr idx_granule,
        const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;

    std::string getDescription() const override;

private:
    struct QueryBbox
    {
        double xmin = 0, ymin = 0, xmax = 0, ymax = 0;
    };

    /// Try to find a spatial predicate in the DAG node tree that filters the indexed column
    /// using a constant geometry. Returns the query bounding box on success.
    std::optional<QueryBbox> extractQueryBbox(
        const ActionsDAG::Node * node,
        const String & col_name);

    /// Outcome of trying to derive a bbox from a single (non-`and`) predicate node.
    enum class NodeBboxStatus
    {
        /// The node isn't a spatial predicate on `col_name` at all (e.g. unrelated function,
        /// or a spatial predicate on a different column) -- carries no pruning information and
        /// cannot force a surrounding conjunction to fail closed.
        NotApplicable,
        /// The node is a spatial predicate on `col_name`, but no safe bbox could be derived
        /// from it (e.g. an extra non-constant argument) -- and there's no reason to think
        /// evaluating it would throw, so a surrounding conjunction may still prune using other
        /// conjuncts' bboxes.
        NoInfo,
        /// The node is a spatial predicate on `col_name` whose constant geometry argument(s)
        /// failed to validate (e.g. an invalid polygon) -- evaluating it is expected to raise
        /// an exception, so pruning based on any other conjunct's bbox could silently skip
        /// that exception.
        Failed,
        /// A bbox was successfully derived.
        Ok,
    };

    /// Walk `node`, recursing only through `and` children, and intersect the bboxes of every
    /// same-column spatial predicate conjunct found (all must hold simultaneously). Sets
    /// `failed` if any conjunct is `NodeBboxStatus::Failed`.
    void collectConjunctiveBbox(
        const ActionsDAG::Node * node,
        const String & col_name,
        bool & has_bbox,
        QueryBbox & bbox,
        bool & failed);

    /// Derive a bbox from a single spatial-predicate node (not itself an `and`).
    NodeBboxStatus extractNodeBbox(
        const ActionsDAG::Node * node,
        const String & col_name,
        QueryBbox & out_bbox);

    String column_name;
    std::optional<QueryBbox> query_bbox;
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

    MergeTreeIndexFormat getDeserializedFormat(
        const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const override;
};

}
