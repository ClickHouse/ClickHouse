#include <Storages/MergeTree/MergeTreeIndexMinMax.h>
#include <DataTypes/TypeTree.h>

#include <Interpreters/ExpressionAnalyzer.h>

#include <Common/FieldAccurateComparison.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>

#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool isBoolOrSimpleAggregateFunctionOfBool(const IDataType & type)
{
    return type.getName() == "Bool" || isSimpleAggregateFunctionOfBool(type);
}

/// How the bounds of an index column have to be read. A `Bool` is stored as a byte that need not be 0 or 1
/// (e.g. after `INSERT ... SELECT toUInt8(2)`), and the index writes that byte as is.
enum BoolBoundsKind : UInt8
{
    /// The column has no `Bool` whose stored byte could be lost.
    NO_BOOL_BOUNDS = 0,
    /// A `Bool`, possibly inside `Nullable`, `LowCardinality` or `SimpleAggregateFunction`.
    SCALAR_BOOL_BOUNDS = 1,
    /// A `Bool` inside a `Tuple`, an `Array` or a `Map`, e.g. `Tuple(Bool)`.
    NESTED_BOOL_BOUNDS = 2,
};

BoolBoundsKind getBoolBoundsKind(const DataTypePtr & type)
{
    const DataTypePtr scalar_type = removeNullable(removeLowCardinality(type));
    if (isBoolOrSimpleAggregateFunctionOfBool(*scalar_type))
        return SCALAR_BOOL_BOUNDS;

    /// A value of `Dynamic`, `Variant` or `JSON` carries the type of what it holds, so a `Bool` in it is compared as
    /// a `Bool` field and has to be read as one. These types are not allowed in a `minmax` index, but the index of a
    /// table created before that check can have them.
    if (anyInTypeTree(*type, [](const IDataType & nested) { return isDynamic(nested) || isVariant(nested) || isObject(nested); }))
        return NO_BOOL_BOUNDS;

    if (anyInTypeTree(*type, isBoolOrSimpleAggregateFunctionOfBool))
        return NESTED_BOOL_BOUNDS;

    return NO_BOOL_BOUNDS;
}

/// Whether a row between the bounds `min` and `max` of a granule can be below `min` once its `Bool` bytes other than
/// 0 and 1 are converted to 1. The bounds are values of `type` read with `read_bool_field_as_int`.
///
/// Values are compared lexicographically, element by element. Take a row with such a byte in the `Bool` at position
/// `p`. It is not below `min`, and the conversion only lowers it at `p`, so it gets below `min` only when it equals
/// `min` before `p` and the `Bool` of `min` at `p` is not 0. The row is not above `max` either. So either `max`
/// already differs from `min` before `p`, or `max` has a byte other than 0 and 1 at `p` too.
///
/// `max` is null once the upper bound differs from the lower bound before the current position.
bool convertedBoolMayBeBelowMin(const Field & min, const Field * & max, const IDataType & type)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&type))
    {
        if (!min.isNull())
            return convertedBoolMayBeBelowMin(min, max, *nullable_type->getNestedType());
    }
    else if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&type))
    {
        return convertedBoolMayBeBelowMin(min, max, *low_cardinality_type->getDictionaryType());
    }
    else if (isBoolOrSimpleAggregateFunctionOfBool(type))
    {
        if (min.getType() == Field::Types::UInt64 && min.safeGet<UInt64>() != 0)
        {
            if (min.safeGet<UInt64>() > 1 || !max || max->getType() != Field::Types::UInt64 || max->safeGet<UInt64>() > 1)
                return true;
        }
    }
    else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(&type); tuple_type && min.getType() == Field::Types::Tuple)
    {
        const auto & min_tuple = min.safeGet<Tuple>();
        const auto & element_types = tuple_type->getElements();
        const Tuple * max_tuple = max && max->getType() == Field::Types::Tuple ? &max->safeGet<Tuple>() : nullptr;
        if (min_tuple.size() != element_types.size() || (max_tuple && max_tuple->size() != min_tuple.size()))
            return true;

        for (size_t i = 0; i < min_tuple.size(); ++i)
        {
            const Field * max_element = max_tuple && max ? &(*max_tuple)[i] : nullptr;
            if (convertedBoolMayBeBelowMin(min_tuple[i], max_element, *element_types[i]))
                return true;
            if (!max_element)
                max = nullptr;
        }
        return false;
    }
    else if (const auto * array_type = typeid_cast<const DataTypeArray *>(&type); array_type && min.getType() == Field::Types::Array)
    {
        const auto & min_array = min.safeGet<Array>();
        const Array * max_array = max && max->getType() == Field::Types::Array ? &max->safeGet<Array>() : nullptr;
        if (!max_array)
            max = nullptr;

        for (size_t i = 0; i < min_array.size(); ++i)
        {
            const Field * max_element = max && i < max_array->size() ? &(*max_array)[i] : nullptr;
            if (convertedBoolMayBeBelowMin(min_array[i], max_element, *array_type->getNestedType()))
                return true;
            if (!max_element)
                max = nullptr;
        }

        if (max && max_array->size() != min_array.size())
            max = nullptr;
        return false;
    }
    else if (const auto * map_type = typeid_cast<const DataTypeMap *>(&type); map_type && min.getType() == Field::Types::Map)
    {
        /// Each element of a `Map` field is a `Tuple` of the key and the value.
        const auto & key_value_type = *assert_cast<const DataTypeArray &>(*map_type->getNestedType()).getNestedType();
        const auto & min_map = min.safeGet<Map>();
        const Map * max_map = max && max->getType() == Field::Types::Map ? &max->safeGet<Map>() : nullptr;
        if (!max_map)
            max = nullptr;

        for (size_t i = 0; i < min_map.size(); ++i)
        {
            const Field * max_element = max && i < max_map->size() ? &(*max_map)[i] : nullptr;
            if (convertedBoolMayBeBelowMin(min_map[i], max_element, key_value_type))
                return true;
            if (!max_element)
                max = nullptr;
        }

        if (max && max_map->size() != min_map.size())
            max = nullptr;
        return false;
    }

    /// A value with no `Bool` inside, a `NULL`, or a `Bool` that cannot lower a row below `min`.
    if (max && *max != min)
        max = nullptr;
    return false;
}

}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
    initSerializations();
}

MergeTreeIndexGranuleMinMax::MergeTreeIndexGranuleMinMax(
    const String & index_name_,
    const Block & index_sample_block_,
    Ranges && hyperrectangle_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , hyperrectangle(std::move(hyperrectangle_))
{
    initSerializations();
}

void MergeTreeIndexGranuleMinMax::initSerializations()
{
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        const DataTypePtr & type = index_sample_block.getByPosition(i).type;
        serializations.push_back(type->getDefaultSerialization());
        bool_columns.push_back(getBoolBoundsKind(type));
    }
    datatypes = index_sample_block.getDataTypes();
    /// A `Bool` column can hold any byte (e.g. `INSERT ... SELECT toUInt8(2)`), so its bounds are read as the stored
    /// bytes rather than converted to `true`, and widened in `deserializeBinary`. The same holds for a `Bool` inside
    /// a `Tuple`, an `Array` or a `Map`. A `Bool` in a `Dynamic`, `Variant` or `JSON` value is compared as a `Bool`
    /// field, so it is still read as one (see `getBoolBoundsKind`).
    bool_format_settings.binary.read_bool_field_as_int = true;
}

void MergeTreeIndexGranuleMinMax::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty minmax index {}", backQuote(index_name));

    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        serializations[i]->serializeBinary(hyperrectangle[i].left, ostr, {});
        serializations[i]->serializeBinary(hyperrectangle[i].right, ostr, {});
    }
}

void MergeTreeIndexGranuleMinMax::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    const size_t num_columns = index_sample_block.columns();

    /// On subsequent calls (when granule is reused), deserialize directly into the existing
    /// Range objects instead of clearing the vector and constructing new Ranges each time.
    /// This avoids repeated vector operations and Field copy-constructions
    /// in hot loops (e.g. skip index evaluation over hundreds of thousands of granules).
    const bool update_in_place = (hyperrectangle.size() == num_columns);

    if (!update_in_place)
        hyperrectangle.clear();

    Field min_val;
    Field max_val;

    for (size_t i = 0; i < num_columns; ++i)
    {
        /// When updating in place, deserialize directly into the Range's fields.
        Field & min_ref = update_in_place ? static_cast<Field &>(hyperrectangle[i].left) : min_val;
        Field & max_ref = update_in_place ? static_cast<Field &>(hyperrectangle[i].right) : max_val;
        const FormatSettings & column_format_settings = bool_columns[i] ? bool_format_settings : format_settings;

        switch (version)
        {
            case 1:
                if (!datatypes[i]->isNullable())
                {
                    serializations[i]->deserializeBinary(min_ref, istr, column_format_settings);
                    serializations[i]->deserializeBinary(max_ref, istr, column_format_settings);
                }
                else
                {
                    /// NOTE: that this serialization differs from
                    /// IMergeTreeDataPart::MinMaxIndex::load() to preserve
                    /// backward compatibility.
                    ///
                    /// But this is deprecated format, so this is OK.

                    bool is_null = false;
                    readBinary(is_null, istr);
                    if (!is_null)
                    {
                        serializations[i]->deserializeBinary(min_ref, istr, column_format_settings);
                        serializations[i]->deserializeBinary(max_ref, istr, column_format_settings);
                    }
                    else
                    {
                        min_ref = Null();
                        max_ref = Null();
                    }
                }
                break;

            /// New format with proper Nullable support for values that include NULL values
            case 2:
                serializations[i]->deserializeBinary(min_ref, istr, column_format_settings);
                serializations[i]->deserializeBinary(max_ref, istr, column_format_settings);

                // NULL_LAST
                if (min_ref.isNull())
                    min_ref = POSITIVE_INFINITY;
                if (max_ref.isNull())
                    max_ref = POSITIVE_INFINITY;

                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
        }

        normalizeBoolFields(min_ref);
        normalizeBoolFields(max_ref);

        /// Some functions see the stored byte of a `Bool` (`b = true`, `toUInt8(b) = 2`), others see it converted to
        /// 0 or 1 (`b IN (true)`). A granule with bytes in `[2, 2]` must match both `toUInt8(b) = 2` and `b IN (true)`,
        /// so its range is widened to `[1, 2]`: the lower bound covers the converted value too. The upper bound
        /// already does, and a granule of only 0 and 1 is not changed.
        if (bool_columns[i] == SCALAR_BOOL_BOUNDS && min_ref.getType() == Field::Types::UInt64 && min_ref.safeGet<UInt64>() > 1)
            min_ref = UInt64(1);

        /// The same for a `Bool` inside a `Tuple`, an `Array` or a `Map`: `(2)` is read as `(2)` or as `(1)`. Such
        /// values are ordered lexicographically, so replacing the byte in place would not give a lower bound for
        /// the elements after it (`(3, 0)` is above `(2, 5)`, but `(1, 0)` is below `(1, 5)`). The lower bound is
        /// dropped instead; the upper bound already covers the converted values, which are never greater.
        ///
        /// The row with the byte need not be the lower bound: a granule of several marks with `(1, 5)` and `(2, 0)`
        /// has the bounds `[(1, 5), (2, 0)]`, and `(2, 0)` converted is `(1, 0)`. So the upper bound is checked as
        /// well. A granule of only 0 and 1 between equal prefixes, e.g. `[(1, 0), (1, 5)]`, keeps its lower bound.
        if (bool_columns[i] == NESTED_BOOL_BOUNDS)
        {
            const Field * max_bound = &max_ref;
            if (convertedBoolMayBeBelowMin(min_ref, max_bound, *datatypes[i]))
                min_ref = NEGATIVE_INFINITY;
        }

        if (update_in_place)
        {
            hyperrectangle[i].left_included = true;
            hyperrectangle[i].right_included = true;
        }
        else
        {
            hyperrectangle.emplace_back(min_val, true, max_val, true);
        }
    }
}

MergeTreeIndexAggregatorMinMax::MergeTreeIndexAggregatorMinMax(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorMinMax::getGranuleAndReset()
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index_name, index_sample_block, std::move(hyperrectangle));
}

void MergeTreeIndexAggregatorMinMax::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    FieldRef field_min;
    FieldRef field_max;
    size_t range_start = *pos;
    size_t range_end = *pos + rows_read;
    for (size_t i = 0; i < index_sample_block.columns(); ++i)
    {
        auto index_column_name = index_sample_block.getByPosition(i).name;
        const auto & src_column = block.getByName(index_column_name).column;
        /// Only LowCardinality needs unwrapping to expose a nested Nullable; gate the call so other
        /// columns are untouched. LC(Nullable(T)) then takes getExtremesNullLast (keeps the +inf NULL
        /// sentinel; otherwise IS NULL wrongly prunes). getExtremes on LC materializes internally too,
        /// so this adds no extra work.
        const auto column = src_column->lowCardinality() ? src_column->convertToFullColumnIfLowCardinality() : src_column;
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
            column_nullable->getExtremesNullLast(field_min, field_max, range_start, range_end);
        else
            column->getExtremes(field_min, field_max, range_start, range_end);

        if (hyperrectangle.size() <= i)
        {
            hyperrectangle.emplace_back(field_min, true, field_max, true);
        }
        else
        {
            hyperrectangle[i].left
                = accurateLess(hyperrectangle[i].left, field_min) ? hyperrectangle[i].left : field_min;
            hyperrectangle[i].right
                = accurateLess(hyperrectangle[i].right, field_max) ? field_max : hyperrectangle[i].right;
        }
    }

    *pos += rows_read;
}

namespace
{

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

}

MergeTreeIndexConditionMinMax::MergeTreeIndexConditionMinMax(
    const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
    : index_data_types(index.data_types)
    , condition(buildCondition(index, filter_dag, context))
{
    /// The granule bound comes from `getExtremes`, which skips NaN.
    condition.relaxAtomsOverNaNHidingColumns(index_data_types);
}

bool MergeTreeIndexConditionMinMax::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        condition.getRPN(),
        {KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE,
         KeyCondition::RPNElement::FUNCTION_IN_RANGE,
         KeyCondition::RPNElement::FUNCTION_IN_SET,
         KeyCondition::RPNElement::FUNCTION_NOT_IN_SET,
         KeyCondition::RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE,
         KeyCondition::RPNElement::FUNCTION_POINT_IN_POLYGON,
         KeyCondition::RPNElement::FUNCTION_ATOM_GROUP,
         KeyCondition::RPNElement::FUNCTION_IS_NULL,
         KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL,
         KeyCondition::RPNElement::ALWAYS_FALSE});
}

bool MergeTreeIndexConditionMinMax::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const
{
    const MergeTreeIndexGranuleMinMax & granule = typeid_cast<const MergeTreeIndexGranuleMinMax &>(*idx_granule);
    return condition.checkInHyperrectangle(granule.hyperrectangle, index_data_types, {}, update_partial_disjunction_result_fn).can_be_true;
}

std::string MergeTreeIndexConditionMinMax::getDescription() const
{
    return condition.getDescription().condition;
}

MergeTreeIndexGranulePtr MergeTreeIndexMinMax::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleMinMax>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexMinMax::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorMinMax>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexMinMax::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context, /* boolean_context */ true);
    return std::make_shared<MergeTreeIndexConditionMinMax>(index, filter_dag, context);
}

MergeTreeIndexFormat MergeTreeIndexMinMax::getPhysicalFormat(
    const MergeTreeDataPartChecksums & checksums, const IDataPartStorage & storage, const std::string & relative_path_prefix) const
{
    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx2", &storage))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};

    if (indexFileExistsInChecksums(checksums, relative_path_prefix, ".idx", &storage))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};

    return {0 /* unknown */, {}};
}

MergeTreeIndexBulkGranulesMinMax::MergeTreeIndexBulkGranulesMinMax(const String & index_name_, const Block & index_sample_block_,
                                                                   size_t index_granularity_, int direction_, size_t size_hint_, size_t last_part_granule_, bool store_map_) :
    index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_granularity(index_granularity_)
    , direction(direction_)
    , last_part_granule(last_part_granule_)
    , store_map(store_map_)
{
    const DataTypePtr & type = index_sample_block.getByPosition(0).type;
    serialization = type->getDefaultSerialization();
    /// Keep the stored bytes of a `Bool` bound, as `MergeTreeIndexGranuleMinMax` does.
    if (getBoolBoundsKind(type) != NO_BOOL_BOUNDS)
        format_settings.binary.read_bool_field_as_int = true;
    granules.reserve(size_hint_);
}

void MergeTreeIndexBulkGranulesMinMax::deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    Field value;
    Field scratch;

    /// The order in which values are read depends on 'direction':
    /// If direction == ASC, we need only min value, discard max value
    /// If direction == DESC, we need only max value, discard min value
    if (direction == 1)
    {
        serialization->deserializeBinary(value, istr, format_settings);
        serialization->deserializeBinary(scratch, istr, format_settings);
    }
    else
    {
        serialization->deserializeBinary(scratch, istr, format_settings);
        serialization->deserializeBinary(value, istr, format_settings);
    }

    normalizeBoolFields(value);

    /// If index granularity is not 1, we insert the same value as the min
    /// or max for all the corresponding granules. For our top-K purpose, this
    /// is safe and maybe lead to false positives, but never wrong results.
    for (size_t i = 0; i < index_granularity; ++i)
    {
        auto part_granule_num = (granule_num * index_granularity) + i;
        if (part_granule_num >= last_part_granule)
            break;

        granules.emplace_back(MinMaxGranule{part_granule_num, value});
        if (store_map)
            granules_map.emplace(part_granule_num, granules.size() - 1);
    }
    empty = false;
}

/// Get top K granules of a single part
template<bool handle_ties>
void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(size_t n, std::vector<MinMaxGranule> & result)
{
    if (n == 0)
        return;

    if (n >= granules.size())
    {
        result.insert(result.end(), granules.begin(), granules.end());
        return;
    }

    std::priority_queue<MinMaxGranuleItem> queue;

    for (const auto & granule : granules)
    {
        if constexpr (!handle_ties) /// more common case
        {
            if (queue.size() < n)
                queue.push({direction, 0, granule.granule_num, granule.min_or_max_value});
            else if ((direction == 1 && granule.min_or_max_value < queue.top().min_or_max_value) ||
                        (direction == -1 && granule.min_or_max_value > queue.top().min_or_max_value))
            {
                queue.pop();
                queue.push({direction, 0, granule.granule_num, granule.min_or_max_value});
            }
        }
        else
        {
            /// we need to return more than 'k' granules
            queue.push({-direction, 0, granule.granule_num, granule.min_or_max_value});
        }
    }

    if constexpr (!handle_ties)
    {
        while (!queue.empty())
        {
            result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
            queue.pop();
        }
    }
    else
    {
        auto min_granules_to_select = n * index_granularity;
        auto threshold = queue.top();
        for (size_t i = 0; i < min_granules_to_select && !queue.empty(); ++i)
        {
            threshold = queue.top();
            result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
            queue.pop();
        }

        while (!queue.empty() && queue.top().min_or_max_value == threshold.min_or_max_value)
        {
            result.push_back({queue.top().granule_num, queue.top().min_or_max_value});
            queue.pop();
        }
    }
}

void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(size_t n, bool handle_ties, std::vector<MinMaxGranule> & result)
{
    if (handle_ties)
        getTopKMarks<true>(n, result);
    else
        getTopKMarks<false>(n, result);
}

/// This routine is for top-N of top-N granules from all parts
template<bool handle_ties>
void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(int direction,
                                                    size_t n,
                                                    size_t index_granularity,
                                                    const std::vector<std::vector<MinMaxGranule>> & parts,
                                                    std::vector<MarkRanges> & result)
{
    if (n == 0)
        return;

    std::priority_queue<MinMaxGranuleItem> queue;

    for (size_t part_index = 0; part_index < parts.size(); ++part_index)
    {
        for (const auto & granule : parts[part_index])
        {
            if constexpr (!handle_ties) /// more common case
            {
                if (queue.size() < n)
                    queue.push({direction, part_index, granule.granule_num, granule.min_or_max_value});
                else if ((direction == 1 && granule.min_or_max_value < queue.top().min_or_max_value) ||
                            (direction == -1 && granule.min_or_max_value > queue.top().min_or_max_value))
                {
                    queue.pop();
                    queue.push({direction, part_index, granule.granule_num, granule.min_or_max_value});
                }
            }
            else
            {
                /// we need to return more than 'k' granules
                queue.push({-direction, part_index, granule.granule_num, granule.min_or_max_value});
            }
        }
    }

    if (queue.empty())
        return;

    result.resize(parts.size(), {});
    if constexpr (!handle_ties)
    {
        while (!queue.empty())
        {
            const auto & item = queue.top();
            result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
            queue.pop();
        }
    }
    else
    {
        auto min_granules_to_select = n * index_granularity;
        auto threshold = queue.top();
        for (size_t i = 0; i < min_granules_to_select && !queue.empty(); ++i)
        {
            const auto & item = queue.top();
            threshold = queue.top();
            result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
            queue.pop();
        }

        while (!queue.empty() && queue.top().min_or_max_value == threshold.min_or_max_value)
        {
            const auto & item = queue.top();
            result[item.part_index].push_back({item.granule_num, item.granule_num + 1});
            queue.pop();
        }
    }

    for (auto & part_ranges : result)
        std::sort(part_ranges.begin(), part_ranges.end());
}

void MergeTreeIndexBulkGranulesMinMax::getTopKMarks(int direction,
                                                    size_t n,
                                                    size_t index_granularity,
                                                    bool handle_ties,
                                                    const std::vector<std::vector<MinMaxGranule>> & parts,
                                                    std::vector<MarkRanges> & result)
{
    if (handle_ties)
        getTopKMarks<true>(direction, n, index_granularity, parts, result);
    else
        getTopKMarks<false>(direction, n, index_granularity, parts, result);
}

MergeTreeIndexPtr minmaxIndexCreator(
    StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & /*settings*/)
{
    return std::make_shared<MergeTreeIndexMinMax>(std::move(metadata_snapshot), index);
}

void minmaxIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & /*settings*/)
{
    if (attach)
        return;

    for (const auto & column : index.sample_block)
    {
        if (!column.type->isComparable())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Data type of argument for minmax index must be comparable, got {} type for column {} instead",
                column.type->getName(), column.name);
        }

        auto check_not_dynamic_or_variant = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "{} data type of column {} is not allowed in minmax index because the values of that data type can contain values "
                    "with different data types. Consider using typed subcolumns or cast column to a specific data type",
                    column.type->getName(), column.name);
        };
        forEachInTypeTree(*column.type, check_not_dynamic_or_variant);
    }
}

}
