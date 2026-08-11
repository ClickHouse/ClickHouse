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

/// Try to extract the bounding box of a constant ActionsDAG COLUMN node
/// containing a native CH geometry (Tuple/Array of Float64 tuples).
/// Delegates to `tryExtractBboxFromColumn` (Common/GeoBbox.h) so that invalid
/// polygons/multipolygons are rejected the same way as the Parquet geo-pruning
/// path, rather than silently producing a bogus bbox that can prune away all
/// granules and hide the "Polygon is not valid" exception `pointInPolygon`
/// would otherwise throw at execute time.
bool tryExtractConstGeoBbox(
    const ActionsDAG::Node * node,
    double & xmin, double & ymin,
    double & xmax, double & ymax)
{
    if (!node->column || !node->is_deterministic_constant)
        return false;

    return tryExtractBboxFromColumn(*node->column, xmin, ymin, xmax, ymax);
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
        query_bbox = extractQueryBbox(predicate, column_name);
}

std::optional<MergeTreeIndexConditionSpatialBbox::QueryBbox>
MergeTreeIndexConditionSpatialBbox::extractQueryBbox(
    const ActionsDAG::Node * node,
    const String & col_name)
{
    if (!node)
        return std::nullopt;

    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        if (node->function_base && node->function_base->isSpatialPredicate()
            && node->children.size() >= 2)
        {
            /// `pointInPolygon` is variadic: besides a single Polygon/MultiPolygon argument,
            /// it also accepts a MultiPolygon spread across several constant polygon
            /// arguments, e.g. pointInPolygon(geom, poly1, poly2, ...), matching a point
            /// that falls in ANY of them. The query bbox must therefore be the union of
            /// all constant geometry children, not just the first one, or granules that
            /// only match a later argument would be incorrectly pruned.
            const ActionsDAG::Node * input_child = nullptr;
            bool has_bbox = false;
            /// Any argument besides the single indexed-column input and constant
            /// geometry literals (e.g. a second column, or a non-constant
            /// expression) means the predicate's truth can depend on something
            /// the bbox can't see — fail closed and disable pruning, matching
            /// Parquet::tryExtractSpatialFilterFromNode's "exactly one
            /// non-constant input" guard.
            bool has_extra_non_constant = false;
            /// A constant geometry child that fails to convert to a bbox (e.g. an invalid
            /// polygon) must not just be skipped: `pointInPolygon` is expected to raise an
            /// exception for it at execute time, so silently unioning the bbox from the
            /// remaining valid children could still prune granules that don't overlap them,
            /// hiding the exception. Matches Parquet::tryExtractSpatialFilterFromNode's
            /// `any_extraction_failed` guard.
            bool any_extraction_failed = false;
            QueryBbox bbox;
            for (const auto * child : node->children)
            {
                if (child->type == ActionsDAG::ActionType::INPUT)
                {
                    if (child->result_name == col_name && !input_child)
                    {
                        input_child = child;
                        continue;
                    }
                    has_extra_non_constant = true;
                    continue;
                }

                if (child->type != ActionsDAG::ActionType::COLUMN || !child->is_deterministic_constant)
                {
                    has_extra_non_constant = true;
                    continue;
                }

                double xmin = 0;
                double ymin = 0;
                double xmax = 0;
                double ymax = 0;
                if (!tryExtractConstGeoBbox(child, xmin, ymin, xmax, ymax))
                {
                    any_extraction_failed = true;
                    continue;
                }

                if (!has_bbox)
                {
                    bbox = {xmin, ymin, xmax, ymax};
                    has_bbox = true;
                }
                else
                {
                    bbox.xmin = std::min(bbox.xmin, xmin);
                    bbox.ymin = std::min(bbox.ymin, ymin);
                    bbox.xmax = std::max(bbox.xmax, xmax);
                    bbox.ymax = std::max(bbox.ymax, ymax);
                }
            }

            if (input_child && has_bbox && !has_extra_non_constant && !any_extraction_failed)
                return bbox;
        }

        /// Only recurse into `and` children: an `or` (or any other function) can be true
        /// via a branch unrelated to the indexed column, so deriving a bbox from just one
        /// of its arguments would incorrectly prune granules.
        if (node->function_base && node->function_base->getName() == "and")
        {
            for (const auto * child : node->children)
            {
                auto result = extractQueryBbox(child, col_name);
                if (result)
                    return result;
            }
        }
    }

    return std::nullopt;
}

bool MergeTreeIndexConditionSpatialBbox::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr idx_granule,
    const UpdatePartialDisjunctionResultFn & /*update_partial_disjunction_result_fn*/) const
{
    if (!query_bbox)
        return true;

    const auto & g = typeid_cast<const MergeTreeIndexGranuleSpatialBbox &>(*idx_granule);
    if (!g.has_data)
        return true; // empty granule — can't prune

    bool disjoint = g.xmax < query_bbox->xmin
                 || g.xmin > query_bbox->xmax
                 || g.ymax < query_bbox->ymin
                 || g.ymin > query_bbox->ymax;
    return !disjoint;
}

std::string MergeTreeIndexConditionSpatialBbox::getDescription() const
{
    if (!query_bbox)
        return "spatial_bbox (no usable predicate)";
    return fmt::format("spatial_bbox bbox=[{},{},{},{}]",
        query_bbox->xmin, query_bbox->ymin, query_bbox->xmax, query_bbox->ymax);
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


MergeTreeIndexFormat MergeTreeIndexSpatialBbox::getDeserializedFormat(
    const IMergeTreeDataPart & part, const std::string & relative_path_prefix) const
{
    for (const auto & [column, _] : getColumnsWithTypesRequiredForIndexCalc())
        if (part.isSystemColumnInvalidated(column))
            return {0 /* unknown */, {}};

    if (indexFileExistsInChecksums(part.checksums, relative_path_prefix, ".idx2", &part.getDataPartStorage()))
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

    /// Accept Point (Tuple(Float64,Float64)), Ring/Polygon/MultiPolygon (nested Arrays),
    /// or any column whose ultimate element type is Tuple(Float64,Float64).
    const DataTypePtr & type = index.data_types[0];
    const IDataType * inner = type.get();

    while (const auto * arr = typeid_cast<const DataTypeArray *>(inner))
        inner = arr->getNestedType().get();

    const auto * tpl = typeid_cast<const DataTypeTuple *>(inner);
    bool is_point_tuple = tpl && tpl->getElements().size() >= 2
        && typeid_cast<const DataTypeFloat64 *>(tpl->getElements()[0].get())
        && typeid_cast<const DataTypeFloat64 *>(tpl->getElements()[1].get());

    if (!is_point_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "spatial_bbox index requires a column of type Point, Ring, Polygon, or MultiPolygon "
            "(Tuple(Float64,Float64) element type). Got: {}",
            type->getName());
}

}
