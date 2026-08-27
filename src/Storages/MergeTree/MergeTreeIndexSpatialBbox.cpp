#include <Storages/MergeTree/MergeTreeIndexSpatialBbox.h>

#include <Columns/ColumnArray.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/GeoBbox.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Recursively accumulate all 2-D points from a native CH geometry column at row `row`.
/// Handles ColumnConst (unwrap), ColumnTuple(Float64,Float64) (point),
/// and ColumnArray (recurse into elements for Ring/Polygon/MultiPolygon).
void addFromGeoColumn(BboxAccumulator & acc, const IColumn & col, size_t row)
{
    if (const auto * const_col = typeid_cast<const ColumnConst *>(&col))
    {
        addFromGeoColumn(acc, const_col->getDataColumn(), 0);
        return;
    }
    if (const auto * tuple_col = typeid_cast<const ColumnTuple *>(&col))
    {
        if (tuple_col->tupleSize() < 2 || row >= tuple_col->size())
            return;
        const auto * x_col = typeid_cast<const ColumnFloat64 *>(&tuple_col->getColumn(0));
        const auto * y_col = typeid_cast<const ColumnFloat64 *>(&tuple_col->getColumn(1));
        if (!x_col || !y_col)
            return;
        acc.add(x_col->getData()[row], y_col->getData()[row]);
        return;
    }
    if (const auto * array_col = typeid_cast<const ColumnArray *>(&col))
    {
        if (row >= array_col->size())
            return;
        const auto & offsets = array_col->getOffsets();
        const size_t start = row > 0 ? offsets[row - 1] : 0;
        const size_t end = offsets[row];
        for (size_t i = start; i < end; ++i)
            addFromGeoColumn(acc, array_col->getData(), i);
    }
}

}


// ─── Granule ─────────────────────────────────────────────────────────────────

MergeTreeIndexGranuleSpatialBbox::MergeTreeIndexGranuleSpatialBbox(const String & index_name_)
    : index_name(index_name_)
{}

void MergeTreeIndexGranuleSpatialBbox::serializeBinary(WriteBuffer & ostr) const
{
    /// A granule can legitimately have no data: all rows in it may be empty
    /// Ring/Polygon/MultiPolygon values (`[]`). Encode that explicitly instead of
    /// throwing, so such granules are stored as non-prunable rather than failing the insert.
    writeBinaryLittleEndian(static_cast<UInt8>(has_data ? 1 : 0), ostr);
    if (!has_data)
        return;

    writeBinaryLittleEndian(xmin, ostr);
    writeBinaryLittleEndian(ymin, ostr);
    writeBinaryLittleEndian(xmax, ostr);
    writeBinaryLittleEndian(ymax, ostr);
}

void MergeTreeIndexGranuleSpatialBbox::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    UInt8 has_data_byte = 0;
    readBinaryLittleEndian(has_data_byte, istr);
    has_data = has_data_byte != 0;
    if (!has_data)
        return;

    readBinaryLittleEndian(xmin, istr);
    readBinaryLittleEndian(ymin, istr);
    readBinaryLittleEndian(xmax, istr);
    readBinaryLittleEndian(ymax, istr);
}


// ─── Aggregator ──────────────────────────────────────────────────────────────

MergeTreeIndexAggregatorSpatialBbox::MergeTreeIndexAggregatorSpatialBbox(
    const String & index_name_, const String & column_name_)
    : index_name(index_name_), column_name(column_name_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSpatialBbox::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleSpatialBbox>(index_name);
    /// acc.valid is false if any non-finite coordinate was seen in the granule. The
    /// finite subset's bbox must not be used for pruning in that case: the spatial
    /// functions reject non-finite coordinates at execute time, and pruning based on
    /// the finite subset's bbox alone could hide that exception. Serialize a
    /// non-prunable ("no data") granule instead, same as the empty-geometry case.
    if (acc.found && acc.valid)
    {
        granule->xmin = acc.xmin;
        granule->ymin = acc.ymin;
        granule->xmax = acc.xmax;
        granule->ymax = acc.ymax;
        granule->has_data = true;
    }
    acc = {};
    rows_seen = 0;
    return granule;
}

void MergeTreeIndexAggregatorSpatialBbox::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index out of bound for spatial_bbox index '{}'", index_name);

    if (!block.has(column_name))
        return;

    const IColumn & col = *block.getByName(column_name).column;
    const size_t rows_read = std::min(limit, block.rows() - *pos);
    for (size_t i = 0; i < rows_read; ++i)
        addFromGeoColumn(acc, col, *pos + i);

    rows_seen += rows_read;
    *pos += rows_read;
}


// ─── Condition ───────────────────────────────────────────────────────────────

MergeTreeIndexConditionSpatialBbox::MergeTreeIndexConditionSpatialBbox(
    const String & column_name_,
    const ActionsDAG::Node * predicate,
    ContextPtr /*context*/)
    : column_name(column_name_)
{
    if (predicate)
        query_bboxes = extractQueryBboxes(predicate, column_name);
}

std::vector<QueryBbox>
MergeTreeIndexConditionSpatialBbox::extractQueryBboxes(
    const ActionsDAG::Node * node,
    const String & col_name)
{
    bool failed = false;
    std::vector<QueryBbox> bboxes;
    std::unordered_set<const ActionsDAG::Node *> visited;

    /// Accept only the INPUT node matching the indexed column: a spatial predicate on any
    /// other column carries no pruning information for THIS index (but its invalid constant
    /// geometry, if any, must still veto pruning -- see extractSpatialPredicateNodeBbox).
    collectConjunctiveSpatialBboxes(
        node, visited,
        [&col_name](const ActionsDAG::Node & child) { return child.result_name == col_name; },
        [&](const ActionsDAG::Node &, const QueryBbox & node_bbox)
        {
            /// One box per conjunct, kept apart: see the comment on `query_bboxes`.
            bboxes.push_back(node_bbox);
        },
        failed);

    if (failed)
        return {};
    return bboxes;
}

bool MergeTreeIndexConditionSpatialBbox::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr idx_granule,
    const UpdatePartialDisjunctionResultFn & /*update_partial_disjunction_result_fn*/) const
{
    if (query_bboxes.empty())
        return true;

    const auto & g = typeid_cast<const MergeTreeIndexGranuleSpatialBbox &>(*idx_granule);
    if (!g.has_data)
        return true; // empty granule — can't prune

    /// The granule survives only if it can satisfy EVERY conjunct, so it is enough for one of them
    /// to be disjoint from the granule bbox to skip it.
    for (const auto & query_bbox : query_bboxes)
    {
        bool disjoint = g.xmax < query_bbox.xmin
                     || g.xmin > query_bbox.xmax
                     || g.ymax < query_bbox.ymin
                     || g.ymin > query_bbox.ymax;
        if (disjoint)
            return false;
    }
    return true;
}

std::string MergeTreeIndexConditionSpatialBbox::getDescription() const
{
    if (query_bboxes.empty())
        return "spatial_bbox (no usable predicate)";
    std::string description = "spatial_bbox bbox=[";
    for (size_t i = 0; i < query_bboxes.size(); ++i)
    {
        const auto & b = query_bboxes[i];
        if (i != 0)
            description += "; ";
        description += fmt::format("{},{},{},{}", b.xmin, b.ymin, b.xmax, b.ymax);
    }
    description += "]";
    return description;
}


// ─── Index ───────────────────────────────────────────────────────────────────

MergeTreeIndexSpatialBbox::MergeTreeIndexSpatialBbox(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexSpatialBbox::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSpatialBbox>(index.name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexSpatialBbox::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSpatialBbox>(
        index.name, index.column_names.at(0));
}

MergeTreeIndexConditionPtr MergeTreeIndexSpatialBbox::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionSpatialBbox>(
        index.column_names.at(0), predicate, context);
}


MergeTreeIndexFormat MergeTreeIndexSpatialBbox::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2", &storage))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};
    return {0 /* unknown */, {}};
}


// ─── Creator & Validator ─────────────────────────────────────────────────────

MergeTreeIndexPtr spatialBboxIndexCreator(
    StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & /*settings*/)
{
    return std::make_shared<MergeTreeIndexSpatialBbox>(std::move(metadata_snapshot), index);
}

void spatialBboxIndexValidator(const IndexDescription & index, bool /*attach*/, const MergeTreeSettings & /*settings*/)
{
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "spatial_bbox index must be defined on exactly one column");

    /// The runtime path (extractQueryBbox) only recognizes a bare ActionsDAG INPUT node
    /// matching the indexed column name -- an index on a computed expression like
    /// tuple(p.1, p.2) would build and store data but could never be used for pruning.
    if (!index.isSimpleSingleColumnIndex())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "spatial_bbox index must be defined on a plain column, not a computed expression");

    /// Accept Point (Tuple(Float64,Float64)), Ring/Polygon/MultiPolygon (nested Arrays),
    /// or any column whose ultimate element type is Tuple(Float64,Float64). The tuple must
    /// have EXACTLY two elements, matching `extractBboxFromFieldValue`'s contract for constant
    /// query geometries: `addFromGeoColumn` (MergeTreeIndexSpatialBbox.cpp) only ever reads the
    /// first two elements of the tuple column, so a wider tuple (e.g. a `Tuple(Float64, Float64,
    /// Float64, Float64)` rect column consumed by an `is_spatial_predicate` UDF as
    /// (xmin, ymin, xmax, ymax)) would silently be indexed as a `Point` from just its first two
    /// coordinates, dropping the extra ones this index would need to prune correctly.
    const DataTypePtr & type = index.data_types[0];
    const IDataType * inner = type.get();

    while (const auto * arr = typeid_cast<const DataTypeArray *>(inner))
        inner = arr->getNestedType().get();

    const auto * tpl = typeid_cast<const DataTypeTuple *>(inner);
    bool is_point_tuple = tpl && tpl->getElements().size() == 2
        && typeid_cast<const DataTypeFloat64 *>(tpl->getElements()[0].get())
        && typeid_cast<const DataTypeFloat64 *>(tpl->getElements()[1].get());

    if (!is_point_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "spatial_bbox index requires a column of type Point, Ring, Polygon, or MultiPolygon "
            "(Tuple(Float64,Float64) element type). Got: {}",
            type->getName());
}

}
