#include <Storages/MergeTree/MergeTreeIndexSpatialBbox.h>

#include <Columns/ColumnArray.h>
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
bool tryExtractConstGeoBbox(
    const ActionsDAG::Node * node,
    double & xmin, double & ymin,
    double & xmax, double & ymax)
{
    if (!node->column || !node->is_deterministic_constant)
        return false;

    const IColumn * raw = node->column.get();
    if (const auto * const_col = typeid_cast<const ColumnConst *>(raw))
        raw = &const_col->getDataColumn();

    BboxAccumulator acc;
    addFromGeoColumn(acc, *raw, 0);
    if (!acc.found)
        return false;

    xmin = acc.xmin;
    ymin = acc.ymin;
    xmax = acc.xmax;
    ymax = acc.ymax;
    return true;
}

} // namespace


// ─── Granule ─────────────────────────────────────────────────────────────────

MergeTreeIndexGranuleSpatialBbox::MergeTreeIndexGranuleSpatialBbox(const String & index_name_)
    : index_name(index_name_)
{}

void MergeTreeIndexGranuleSpatialBbox::serializeBinary(WriteBuffer & ostr) const
{
    if (!has_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Attempt to serialize empty spatial_bbox index granule for index '{}'", index_name);

    writeBinaryLittleEndian(xmin, ostr);
    writeBinaryLittleEndian(ymin, ostr);
    writeBinaryLittleEndian(xmax, ostr);
    writeBinaryLittleEndian(ymax, ostr);
}

void MergeTreeIndexGranuleSpatialBbox::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    readBinaryLittleEndian(xmin, istr);
    readBinaryLittleEndian(ymin, istr);
    readBinaryLittleEndian(xmax, istr);
    readBinaryLittleEndian(ymax, istr);
    has_data = true;
}


// ─── Aggregator ──────────────────────────────────────────────────────────────

MergeTreeIndexAggregatorSpatialBbox::MergeTreeIndexAggregatorSpatialBbox(
    const String & index_name_, const String & column_name_)
    : index_name(index_name_), column_name(column_name_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSpatialBbox::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleSpatialBbox>(index_name);
    if (acc.found)
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
            const ActionsDAG::Node * input_child = nullptr;
            const ActionsDAG::Node * const_child = nullptr;
            for (const auto * child : node->children)
            {
                if (child->type == ActionsDAG::ActionType::INPUT
                    && child->result_name == col_name
                    && !input_child)
                    input_child = child;
                else if (child->type == ActionsDAG::ActionType::COLUMN
                         && child->is_deterministic_constant
                         && !const_child)
                    const_child = child;
            }

            if (input_child && const_child)
            {
                QueryBbox bbox;
                if (tryExtractConstGeoBbox(const_child,
                    bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax))
                    return bbox;
            }
        }

        /// Recurse into AND children to find qualifying sub-predicates.
        for (const auto * child : node->children)
        {
            auto result = extractQueryBbox(child, col_name);
            if (result)
                return result;
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

MergeTreeIndexSpatialBbox::MergeTreeIndexSpatialBbox(const IndexDescription & index_)
    : IMergeTreeIndex(index_)
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
    const MergeTreeDataPartChecksums & checksums,
    const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2"))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};
    return {0 /* unknown */, {}};
}


// ─── Creator & Validator ─────────────────────────────────────────────────────

MergeTreeIndexPtr spatialBboxIndexCreator(const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexSpatialBbox>(index);
}

void spatialBboxIndexValidator(const IndexDescription & index, bool /*attach*/)
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
