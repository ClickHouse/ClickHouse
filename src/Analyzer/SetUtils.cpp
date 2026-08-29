#include <Analyzer/SetUtils.h>

#include <Core/Block.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <Interpreters/Set.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertColumnToType.h>

#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_ELEMENT_OF_SET;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int UNKNOWN_ELEMENT_OF_ENUM;
}

namespace
{

/// Unwrap Nullable to get to the underlying Tuple type.
/// Also unwrap LowCardinality for robustness, even though LowCardinality(Tuple) is not supported.
const DataTypeTuple * getTupleType(const DataTypePtr & type)
{
    const IDataType * current = type.get();

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(current))
        current = lc_type->getDictionaryType().get();

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(current))
        current = nullable_type->getNestedType().get();

    return typeid_cast<const DataTypeTuple *>(current);
}

size_t getCompoundTypeDepth(const IDataType & type)
{
    size_t depth = 0;

    const IDataType * current_type = &type;

    while (true)
    {
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(current_type))
        {
            current_type = nullable_type->getNestedType().get();
            continue;
        }

        WhichDataType which_type(*current_type);

        if (which_type.isArray())
        {
            current_type = assert_cast<const DataTypeArray &>(*current_type).getNestedType().get();
            ++depth;
        }
        else if (which_type.isTuple())
        {
            const auto & tuple_elements = assert_cast<const DataTypeTuple &>(*current_type).getElements();
            ++depth;
            if (tuple_elements.empty())
                break;
            current_type = tuple_elements.front().get();
        }
        else
        {
            break;
        }
    }

    return depth;
}

/// A single set member as a size-1 column of `type` (its value is at row 0).
struct SetMember
{
    ColumnPtr column;
    DataTypePtr type;
};
using SetMembers = std::vector<SetMember>;

/// Column-native twin of the previous `convertFieldToTypeCheckEnum`. Converts row 0 of the size-1
/// `member` column of type `from_type` into `to_type` with `strict=true` (so values not exactly
/// representable in `to_type` are excluded from the set - e.g. `33.33 :: Decimal(9,2)` for a
/// `Decimal(9,1)` column). Returns:
///   - `std::nullopt` to SKIP the member: not representable (`convertColumnToTypeOrNull` returned {}),
///     or an unknown enum literal when `forbid_unknown_enum_values` is false;
///   - otherwise a size-1 column of `to_type` (which may hold NULL for a genuine NULL member).
std::optional<ColumnPtr> convertColumnToTypeCheckEnum(
    const IColumn & member, const DataTypePtr & from_type, const DataTypePtr & to_type, bool forbid_unknown_enum_values)
{
    try
    {
        ColumnPtr result = convertColumnToTypeOrNull(member, from_type, to_type, {}, /*strict=*/true);
        if (!result)
            return std::nullopt;
        return result;
    }
    catch (const Exception & e)
    {
        if (!forbid_unknown_enum_values && isEnum(to_type) && e.code() == ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM)
            return std::nullopt;
        throw;
    }
}

/// Whether a whole `source`-typed column can be converted to `lhs_type` with one accurate batch cast
/// instead of per-element conversion: both must be plain native numbers (no
/// `Bool`/`Decimal`/`Enum`/`Nullable`/wide-int). This is exactly the case where
/// `castColumnAccurateOrNull` provably matches the strict per-element `convertColumnToType` (pinned by
/// `gtest_convert_column_to_type`).
bool nativeBatchApplicable(const DataTypePtr & source_type, const DataTypePtr & lhs_type)
{
    return isNativeNumber(source_type) && isNativeNumber(lhs_type) && !isBool(source_type) && !isBool(lhs_type);
}

/// Build an `IColumn::filter` (1 = keep) that drops the NULL rows of `null_map` (keeps row `i` where
/// `null_map[i] == 0`); `kept` receives the number of kept rows.
IColumn::Filter keepNotNullFilter(const NullMap & null_map, size_t & kept)
{
    IColumn::Filter filter(null_map.size());
    kept = 0;
    for (size_t i = 0, n = null_map.size(); i < n; ++i)
    {
        filter[i] = null_map[i] ? 0 : 1;
        kept += filter[i];
    }
    return filter;
}

/// The members of one key position converted to `lhs_type`, in member order: `values` holds one row per
/// member (a default row where the member is dropped) and `keep[i]` says whether member `i` enters the set.
struct ConvertedMembers
{
    MutableColumnPtr values;
    IColumn::Filter keep;
    size_t kept = 0;
};

/// Convert the members of one key position to `lhs_type`, preserving their order. A literal list such
/// as `(1, 300, 70000)` is heterogeneously typed (`UInt8`, `UInt16`, `UInt32`), so the members are grouped
/// by type and every native-number group is converted with a single accurate cast: a per-member
/// `castColumnAccurateOrNull` would build a CAST function for every member, which is prohibitive for the
/// tens of thousands of literals a direct dictionary sends per block. All other groups keep the
/// per-member `convertColumnToTypeCheckEnum` path, so their semantics and errors are unchanged.
///
/// A member is dropped when it is not representable in `lhs_type`, or when it is a genuine NULL and NULLs
/// are not inserted (`transform_null_in` off, or a non-nullable target).
ConvertedMembers convertMembers(const SetMembers & members, const DataTypePtr & lhs_type, GetSetElementParams params)
{
    const size_t n = members.size();
    const bool insert_null = params.transform_null_in && lhs_type->isNullable();

    std::vector<size_t> group_of(n);
    DataTypes group_types;
    std::vector<std::vector<size_t>> group_members;
    for (size_t i = 0; i < n; ++i)
    {
        size_t g = 0;
        while (g < group_types.size() && !group_types[g]->equals(*members[i].type))
            ++g;
        if (g == group_types.size())
        {
            group_types.push_back(members[i].type);
            group_members.emplace_back();
        }
        group_members[g].push_back(i);
        group_of[i] = g;
    }

    /// Per group: the kept values (of `lhs_type`, in member order) and a keep flag per group member.
    const size_t num_groups = group_types.size();
    Columns group_values(num_groups);
    std::vector<IColumn::Filter> group_keep(num_groups);
    for (size_t g = 0; g < num_groups; ++g)
    {
        const auto & indices = group_members[g];
        const DataTypePtr & source_type = group_types[g];

        if (nativeBatchApplicable(source_type, lhs_type))
        {
            MutableColumnPtr gathered = source_type->createColumn();
            gathered->reserve(indices.size());
            for (size_t i : indices)
                gathered->insertRangeFrom(*members[i].column, 0, 1);

            ColumnPtr casted = castColumnAccurateOrNull({std::move(gathered), source_type, ""}, lhs_type);
            casted = casted->convertToFullColumnIfConst();
            const auto & nullable = assert_cast<const ColumnNullable &>(*casted);

            size_t kept = 0;
            group_keep[g] = keepNotNullFilter(nullable.getNullMapData(), kept);
            group_values[g] = nullable.getNestedColumn().filter(group_keep[g], kept);
            continue;
        }

        MutableColumnPtr values = lhs_type->createColumn();
        IColumn::Filter & keep = group_keep[g];
        keep.resize(indices.size());
        for (size_t pos = 0; pos < indices.size(); ++pos)
        {
            const auto & member = members[indices[pos]];
            auto converted = convertColumnToTypeCheckEnum(*member.column, member.type, lhs_type, params.forbid_unknown_enum_values);
            keep[pos] = converted && (!(*converted)->isNullAt(0) || insert_null);
            if (keep[pos])
                values->insertRangeFrom(**converted, 0, 1);
        }
        group_values[g] = std::move(values);
    }

    ConvertedMembers result{lhs_type->createColumn(), IColumn::Filter(n), 0};
    result.values->reserve(n);
    std::vector<size_t> member_cursor(num_groups, 0);
    std::vector<size_t> value_cursor(num_groups, 0);
    for (size_t i = 0; i < n; ++i)
    {
        const size_t g = group_of[i];
        const UInt8 k = group_keep[g][member_cursor[g]++];
        result.keep[i] = k;
        result.kept += k;
        if (k)
            result.values->insertFrom(*group_values[g], value_cursor[g]++);
        else
            result.values->insertDefault();
    }
    return result;
}

/// Unwrap a non-NULL member column down to its `ColumnTuple` (the member holds a Tuple value at row 0).
const ColumnTuple & getMemberTuple(const ColumnPtr & member_column, ColumnPtr & holder)
{
    holder = member_column->convertToFullColumnIfConst();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(holder.get()))
        holder = nullable->getNestedColumnPtr();
    return assert_cast<const ColumnTuple &>(*holder);
}

/// Build the converted set columns from the set members (each a size-1 column). Column-native
/// counterpart of the previous `Field`-based `createBlockFromCollection`.
///
/// members can be:
/// - single-key: [1], [2], [3] (each member a scalar), or Tuple members for a `Nullable(Tuple)` LHS;
/// - multi-key: each member a Tuple that is unpacked into `lhs_unpacked_types.size()` columns.
ColumnsWithTypeAndName createBlockFromCollection(
    const SetMembers & members, const DataTypes & lhs_unpacked_types, GetSetElementParams params)
{
    size_t num_elements = lhs_unpacked_types.size();

    /// Fast path: single key column (lhs_unpacked_types.size() == 1)
    /// Special-case `Nullable(Tuple(...))`; otherwise generic scalar conversion.
    if (num_elements == 1)
    {
        const auto & lhs_type = lhs_unpacked_types[0];

        /// For `Nullable(Tuple(...))` we process tuple members element-by-element:
        /// - to correctly skip unknown enum literals when `validate_enum_literals_in_operators = 0`
        /// - to implement `transform_null_in = 0` semantics for NULLs inside tuple elements (skip such tuple values)
        const auto * lhs_nullable = typeid_cast<const DataTypeNullable *>(lhs_type.get());
        const auto * lhs_tuple = lhs_nullable ? typeid_cast<const DataTypeTuple *>(lhs_nullable->getNestedType().get()) : nullptr;

        /// Nullable(Tuple(...)) case
        if (lhs_tuple)
        {
            const auto & lhs_tuple_element_types = lhs_tuple->getElements();
            MutableColumnPtr column = lhs_type->createColumn();
            column->reserve(members.size());
            auto & nullable_column = assert_cast<ColumnNullable &>(*column);

            for (const auto & member : members)
            {
                /// The NULL can be of any type but that's okay
                if (member.column->isNullAt(0))
                {
                    if (params.transform_null_in)
                        column->insert(Null{});
                    continue;
                }

                const DataTypeTuple * rhs_tuple_type = getTupleType(member.type);
                if (!rhs_tuple_type)
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                        "Invalid element type in set. Expected Tuple, got {}", member.type->getName());

                const DataTypes & rhs_tuple_element_types = rhs_tuple_type->getElements();

                if (rhs_tuple_element_types.size() != lhs_tuple_element_types.size())
                    throw Exception(
                        ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                        "Incorrect size of tuple in set: {} instead of {}",
                        rhs_tuple_element_types.size(),
                        lhs_tuple_element_types.size());

                ColumnPtr member_holder;
                const ColumnTuple & member_tuple = getMemberTuple(member.column, member_holder);

                Columns converted_elements;
                converted_elements.reserve(rhs_tuple_element_types.size());

                bool skip_tuple_value = false;
                for (size_t i = 0; i < rhs_tuple_element_types.size(); ++i)
                {
                    auto converted = convertColumnToTypeCheckEnum(
                        *member_tuple.getColumnPtr(i),
                        rhs_tuple_element_types[i],
                        lhs_tuple_element_types[i],
                        params.forbid_unknown_enum_values);

                    if (!converted)
                    {
                        skip_tuple_value = true;
                        break;
                    }

                    bool need_insert_null = params.transform_null_in && lhs_tuple_element_types[i]->isNullable();
                    if ((*converted)->isNullAt(0) && !need_insert_null)
                    {
                        skip_tuple_value = true;
                        break;
                    }

                    converted_elements.push_back(std::move(*converted));
                }

                if (skip_tuple_value)
                    continue;

                /// `ColumnTuple::create` rejects a zero-column tuple, so an empty `Nullable(Tuple())`
                /// element is inserted as a default (empty) tuple row directly.
                if (converted_elements.empty())
                    nullable_column.getNestedColumn().insertDefault();
                else
                {
                    auto tuple_column = ColumnTuple::create(std::move(converted_elements));
                    nullable_column.getNestedColumn().insertRangeFrom(*tuple_column, 0, 1);
                }
                nullable_column.getNullMapData().push_back(UInt8(0));
            }

            ColumnsWithTypeAndName res(1);
            res[0].type = lhs_type;
            res[0].column = std::move(column);
            return res;
        }

        /// Generic single-key column (all cases except `Nullable(Tuple(...))`, e.g. T / Nullable(T) / Tuple() / Tuple(T))
        ConvertedMembers converted = convertMembers(members, lhs_type, params);

        ColumnsWithTypeAndName res(1);
        res[0].type = lhs_type;
        if (converted.kept == members.size())
            res[0].column = std::move(converted.values);
        else
            res[0].column = converted.values->filter(converted.keep, converted.kept);
        return res;
    }

    /// Multi-column key: unpack every non-NULL tuple member into its element columns.
    std::vector<ColumnPtr> member_holders;
    std::vector<const ColumnTuple *> member_tuples;
    std::vector<const DataTypes *> member_element_types;
    member_holders.reserve(members.size());
    member_tuples.reserve(members.size());
    member_element_types.reserve(members.size());

    for (const auto & member : members)
    {
        if (member.column->isNullAt(0))
            continue;

        const DataTypeTuple * tuple_type = getTupleType(member.type);
        if (!tuple_type)
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                "Invalid element type in set. Expected Tuple, got {}", member.type->getName());

        const DataTypes & rhs_element_unpacked_types = tuple_type->getElements();

        if (rhs_element_unpacked_types.size() != num_elements)
            throw Exception(
                ErrorCodes::INCORRECT_ELEMENT_OF_SET,
                "Incorrect size of tuple in set: {} instead of {}", rhs_element_unpacked_types.size(), num_elements);

        member_holders.emplace_back();
        member_tuples.push_back(&getMemberTuple(member.column, member_holders.back()));
        member_element_types.push_back(&rhs_element_unpacked_types);
    }

    /// Convert key by key, each time only the tuples that every previous key kept: this preserves the
    /// per-tuple short-circuit of the per-member loop (a later key of a tuple already dropped by an earlier
    /// key is never converted, so it can neither throw nor be observed).
    const size_t num_tuples = member_tuples.size();
    std::vector<UInt8> alive(num_tuples, 1);
    std::vector<ColumnPtr> key_values(num_elements);
    std::vector<std::vector<size_t>> key_tuple_indices(num_elements);

    for (size_t key = 0; key < num_elements; ++key)
    {
        SetMembers key_members;
        auto & tuple_indices = key_tuple_indices[key];
        for (size_t t = 0; t < num_tuples; ++t)
        {
            if (!alive[t])
                continue;
            key_members.push_back({member_tuples[t]->getColumnPtr(key), (*member_element_types[t])[key]});
            tuple_indices.push_back(t);
        }

        ConvertedMembers converted = convertMembers(key_members, lhs_unpacked_types[key], params);
        for (size_t r = 0; r < tuple_indices.size(); ++r)
            if (!converted.keep[r])
                alive[tuple_indices[r]] = 0;
        key_values[key] = std::move(converted.values);
    }

    ColumnsWithTypeAndName res(num_elements);
    for (size_t key = 0; key < num_elements; ++key)
    {
        const auto & tuple_indices = key_tuple_indices[key];
        IColumn::Filter filter(tuple_indices.size());
        size_t kept = 0;
        for (size_t r = 0; r < tuple_indices.size(); ++r)
        {
            filter[r] = alive[tuple_indices[r]];
            kept += filter[r];
        }

        res[key].type = lhs_unpacked_types[key];
        res[key].column = kept == tuple_indices.size() ? key_values[key] : key_values[key]->filter(filter, kept);
    }

    return res;
}

/// Build set members from an `Array` collection column (size-1, holds one array): the members are the
/// array's elements, all of the array's nested type.
SetMembers membersFromArray(const ColumnPtr & rhs_column, const DataTypePtr & array_type)
{
    const auto & nested_type = assert_cast<const DataTypeArray &>(*array_type).getNestedType();
    ColumnPtr full = rhs_column->convertToFullColumnIfConst();
    const auto & array_column = assert_cast<const ColumnArray &>(*full);
    const ColumnPtr & elements = array_column.getDataPtr();

    SetMembers members;
    size_t size = elements->size();
    members.reserve(size);
    for (size_t i = 0; i < size; ++i)
        members.push_back({elements->cut(i, 1), nested_type});
    return members;
}

/// Single native key (`x IN [...]`): batch-convert the whole key column with one accurate cast, then
/// emit the set column honoring NULLs. `source` is the (non-nullable) native value column and
/// `element_null` (may be null) marks genuine NULL array elements - i.e. a `Nullable(native)` element
/// type. `source_type`/`lhs_type` may be `Nullable`; only their inner native types matter for the cast.
///
/// Per row, matching the general per-element single-key loop exactly:
///   - genuine NULL element -> insert NULL iff `transform_null_in` and the target is nullable, else skip;
///   - not-representable value (NULL in the accurate-cast result) -> skip;
///   - otherwise -> insert the converted value.
ColumnPtr buildSingleNativeKey(
    const ColumnPtr & source, const DataTypePtr & source_type, const DataTypePtr & lhs_type,
    const NullMap * element_null, bool transform_null_in)
{
    ColumnPtr casted = castColumnAccurateOrNull({source, removeNullable(source_type), ""}, removeNullable(lhs_type));
    casted = casted->convertToFullColumnIfConst();
    const auto & casted_nullable = assert_cast<const ColumnNullable &>(*casted);
    const NullMap & casted_null = casted_nullable.getNullMapData();

    const size_t n = source->size();
    const bool lhs_nullable = lhs_type->isNullable();

    /// Build the keep filter (and, for a nullable target, the result null-map) in one pass, then apply a
    /// single vectorized `filter`. This mirrors the general per-element loop: a genuine NULL element is
    /// kept as NULL iff `transform_null_in` and the target is nullable (else dropped); a not-representable
    /// value (NULL in the accurate cast) is dropped; otherwise the converted value is kept.
    IColumn::Filter keep(n);
    auto result_null_map = lhs_nullable ? ColumnUInt8::create() : ColumnUInt8::MutablePtr{};
    NullMap * result_null_data = result_null_map ? &result_null_map->getData() : nullptr;
    size_t kept = 0;
    for (size_t row = 0; row < n; ++row)
    {
        const bool genuine_null = element_null && (*element_null)[row];
        UInt8 k = genuine_null ? static_cast<UInt8>(transform_null_in && lhs_nullable) : static_cast<UInt8>(!casted_null[row]);
        keep[row] = k;
        if (k)
        {
            ++kept;
            if (result_null_data)
                result_null_data->push_back(static_cast<UInt8>(genuine_null));
        }
    }

    ColumnPtr nested_kept = casted_nullable.getNestedColumn().filter(keep, kept);
    if (!lhs_nullable)
        return nested_kept;
    return ColumnNullable::create(nested_kept, std::move(result_null_map));
}

/// Batch-convert `k > 1` parallel source key columns (each `n` rows) into `target_types` with one
/// accurate cast per key, then keep only the rows where every key converts. Precondition: every
/// `nativeBatchApplicable(source_types[j], target_types[j])` (in particular every target is a
/// non-nullable native number).
///
/// A set row (one key tuple) is kept iff it is not an array-element NULL (`element_null`, for an
/// `Array(Nullable(Tuple))` RHS; may be null) and every key value is exactly representable in its target
/// (a NULL in the accurate-cast result means not-representable; native targets are non-nullable so
/// `transform_null_in` never keeps a NULL). This matches the general per-element multi-key loop exactly,
/// but converts each key column as a whole instead of cutting per element.
ColumnsWithTypeAndName buildNativeKeysBatch(
    const Columns & sources, const DataTypes & source_types, const DataTypes & target_types, const NullMap * element_null)
{
    const size_t k = sources.size();
    const size_t n = sources.empty() ? 0 : sources[0]->size();

    std::vector<const ColumnNullable *> casted(k);
    Columns casted_holder(k);
    for (size_t j = 0; j < k; ++j)
    {
        ColumnPtr c = castColumnAccurateOrNull({sources[j], source_types[j], ""}, target_types[j]);
        casted_holder[j] = c->convertToFullColumnIfConst();
        casted[j] = &assert_cast<const ColumnNullable &>(*casted_holder[j]);
    }

    /// Build one shared keep filter (a row/tuple is kept iff it is not an array-element NULL and every
    /// key is representable), then apply a single vectorized `filter` per key column.
    IColumn::Filter keep(n);
    size_t kept = 0;
    for (size_t row = 0; row < n; ++row)
    {
        bool drop = element_null && (*element_null)[row];
        for (size_t j = 0; !drop && j < k; ++j)
            drop = casted[j]->isNullAt(row);
        keep[row] = drop ? 0 : 1;
        kept += keep[row];
    }

    ColumnsWithTypeAndName res(k);
    for (size_t j = 0; j < k; ++j)
    {
        res[j].type = target_types[j];
        res[j].column = casted[j]->getNestedColumn().filter(keep, kept);
    }
    return res;
}

/// Columnar fast path for an `Array` RHS whose key columns are all native numbers - single scalar key
/// (`x IN [...]`, including a `Nullable` element type and/or a `Nullable` key) or a multi-column key over
/// an array of tuples (`(a, b) IN [(1, 2), (3, 4)]`). Because an `Array` is homogeneous, every element
/// shares one nested type, so the `k` key values are already laid out as `k` contiguous columns (the
/// array's data column, or the sub-columns of its data `Tuple`) - converted directly, without
/// `membersFromArray` cutting each element into a size-1 column. Returns nullopt (caller uses the general
/// per-element path, preserving its exact errors) unless every target key column is native (ignoring an
/// outer `Nullable`) and the RHS shape lines up.
std::optional<ColumnsWithTypeAndName> tryBuildFromNativeArray(
    const ColumnPtr & rhs_column, const DataTypePtr & nested_type, const DataTypes & lhs_unpacked_types,
    GetSetElementParams params)
{
    const size_t k = lhs_unpacked_types.size();

    ColumnPtr full = rhs_column->convertToFullColumnIfConst();
    const ColumnPtr & data = assert_cast<const ColumnArray &>(*full).getDataPtr();

    if (k == 1)
    {
        /// Single scalar key: the array's data column is the only key column. Multi-key handling below
        /// requires non-nullable native keys, but a single key can be `Nullable` on either side - the
        /// per-row NULL policy is handled by `buildSingleNativeKey`.
        const DataTypePtr & lhs_type = lhs_unpacked_types[0];
        if (!nativeBatchApplicable(removeNullable(nested_type), removeNullable(lhs_type)))
            return std::nullopt;

        /// Unwrap a `Nullable` element type into the inner native column + the element null-map.
        ColumnPtr source = data;
        const NullMap * element_null = nullptr;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(data.get()))
        {
            element_null = &nullable->getNullMapData();
            source = nullable->getNestedColumnPtr();
        }

        return ColumnsWithTypeAndName{{buildSingleNativeKey(source, nested_type, lhs_type, element_null, params.transform_null_in), lhs_type, ""}};
    }

    {
        Columns sources;
        DataTypes source_types;
        const NullMap * element_null = nullptr;
        /// Multi-column key: the elements must be tuples of matching arity, all key columns native.
        DataTypePtr inner = nested_type;
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(inner.get()))
            inner = nullable_type->getNestedType();
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(inner.get());
        if (!tuple_type)
            return std::nullopt;

        const auto & element_types = tuple_type->getElements();
        if (element_types.size() != k)
            return std::nullopt;
        for (size_t j = 0; j < k; ++j)
            if (!nativeBatchApplicable(element_types[j], lhs_unpacked_types[j]))
                return std::nullopt;

        const IColumn * data_col = data.get();
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(data_col))
        {
            element_null = &nullable->getNullMapData();
            data_col = &nullable->getNestedColumn();
        }
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*data_col);

        sources.reserve(k);
        source_types.reserve(k);
        for (size_t j = 0; j < k; ++j)
        {
            sources.push_back(tuple_column.getColumnPtr(j));
            source_types.push_back(element_types[j]);
        }

        return buildNativeKeysBatch(sources, source_types, lhs_unpacked_types, element_null);
    }
}

/// Build set members from a `Tuple` collection column (size-1, holds one tuple): the members are the
/// tuple's elements (each a size-1 column), with the tuple element types.
SetMembers membersFromTuple(const ColumnPtr & rhs_column, const DataTypePtr & tuple_type)
{
    const auto & element_types = assert_cast<const DataTypeTuple &>(*tuple_type).getElements();
    ColumnPtr full = rhs_column->convertToFullColumnIfConst();
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*full);

    SetMembers members;
    members.reserve(element_types.size());
    for (size_t i = 0; i < element_types.size(); ++i)
        members.push_back({tuple_column.getColumnPtr(i), element_types[i]});
    return members;
}

/// A single-value collection: the RHS itself is the only member.
SetMembers membersFromSingleValue(const ColumnPtr & rhs_column, const DataTypePtr & rhs_type)
{
    return {{rhs_column->convertToFullColumnIfConst(), rhs_type}};
}

/// Whether any element of the collection column (Array or Tuple) is NULL.
bool columnCollectionHasNull(const ColumnPtr & rhs_column, WhichDataType rhs_which_type)
{
    ColumnPtr full = rhs_column->convertToFullColumnIfConst();
    if (rhs_which_type.isArray())
    {
        const auto & elements = assert_cast<const ColumnArray &>(*full).getData();
        for (size_t i = 0, size = elements.size(); i < size; ++i)
            if (elements.isNullAt(i))
                return true;
        return false;
    }
    if (rhs_which_type.isTuple())
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*full);
        for (size_t i = 0, size = tuple.tupleSize(); i < size; ++i)
            if (tuple.getColumn(i).isNullAt(0))
                return true;
        return false;
    }
    return false;
}

/// Whether any non-NULL element of the collection (Array or Tuple) holds a Tuple value.
/// A column's elements are homogeneously typed, so "is a Tuple" is a type check on the element type;
/// but the value must also be non-NULL to match the `Field`-based version, where a NULL value has
/// `Field::Types::Null` (not `Tuple`) regardless of the declared element type.
bool columnCollectionHasTuple(const ColumnPtr & rhs_column, const DataTypePtr & rhs_type, WhichDataType rhs_which_type)
{
    ColumnPtr full = rhs_column->convertToFullColumnIfConst();
    if (rhs_which_type.isArray())
    {
        if (getTupleType(assert_cast<const DataTypeArray &>(*rhs_type).getNestedType()) == nullptr)
            return false;
        const auto & elements = assert_cast<const ColumnArray &>(*full).getData();
        for (size_t i = 0, size = elements.size(); i < size; ++i)
            if (!elements.isNullAt(i))
                return true;
        return false;
    }
    if (rhs_which_type.isTuple())
    {
        const auto & element_types = assert_cast<const DataTypeTuple &>(*rhs_type).getElements();
        const auto & tuple = assert_cast<const ColumnTuple &>(*full);
        for (size_t i = 0, size = tuple.tupleSize(); i < size; ++i)
            if (getTupleType(element_types[i]) != nullptr && !tuple.getColumn(i).isNullAt(0))
                return true;
    }
    return false;
}

}

/// Format: lhs IN rhs
/// Explanation of the setting: `transform_null_in`. First of all, it is only applicable if the lhs is nullable.
/// Then if lhs is nullable and `transform_null_in` is true, then NULLs from rhs are inserted into the result set as well.
/// Whereas, if `transform_null_in` is false, we pretend NULLs are not present in rhs at all (at level 1 or at level 2 for Tuple).
/// If `transform_null_in` is false, then `SELECT NULL IN (NULL, 1)` returns NULL, otherwise it returns true.

ColumnsWithTypeAndName getSetElementsForConstantValue(
    const DataTypePtr & lhs_expression_type, const ColumnPtr & rhs_column, const DataTypePtr & rhs_type, GetSetElementParams params)
{
    DataTypes lhs_unpacked_types = {lhs_expression_type};

    /// Unpack `Tuple(...)` into tuple elements.
    /// For `Nullable(Tuple(...))` we keep it as a single value and handle it in createBlockFromCollection() fast-path.
    bool lhs_is_tuple = false;
    const auto * lhs_nullable_type = typeid_cast<const DataTypeNullable *>(lhs_expression_type.get());
    const auto * lhs_tuple_type
        = typeid_cast<const DataTypeTuple *>(lhs_nullable_type ? lhs_nullable_type->getNestedType().get() : lhs_expression_type.get());

    if (lhs_tuple_type)
    {
        lhs_is_tuple = true;

        /// Do not unpack empty tuple or single element tuple.
        /// Do not unpack `Nullable(Tuple(...))` because in the end we build a single `Nullable(Tuple(...))` column anyway.
        if (!lhs_nullable_type && lhs_tuple_type->getElements().size() > 1)
            lhs_unpacked_types = lhs_tuple_type->getElements();
    }

    bool lhs_is_nullable = (lhs_nullable_type != nullptr);

    for (auto & lhs_element_type : lhs_unpacked_types)
    {
        if (const auto * set_element_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(lhs_element_type.get()))
            lhs_element_type = set_element_low_cardinality_type->getDictionaryType();
    }

    /// If we didn't unpack `Nullable(Tuple(...))`, we still need to remove `LowCardinality` from tuple elements
    /// to match the behavior of the unpacked path.
    if (lhs_tuple_type && lhs_nullable_type && lhs_tuple_type->getElements().size() > 1)
    {
        DataTypes nested_tuple_element_types = lhs_tuple_type->getElements();
        for (auto & element_type : nested_tuple_element_types)
        {
            if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(element_type.get()))
                element_type = low_cardinality_type->getDictionaryType();
        }

        DataTypePtr nested_tuple_type = lhs_tuple_type->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(nested_tuple_element_types, lhs_tuple_type->getElementNames())
            : std::make_shared<DataTypeTuple>(nested_tuple_element_types);
        lhs_unpacked_types = {std::make_shared<DataTypeNullable>(nested_tuple_type)};
    }

    auto build_from_array = [&](const DataTypePtr & type)
    {
        /// Hot path: an `Array` RHS whose key columns are all native numbers (single scalar key, or a
        /// multi-column key over an array of tuples). The array is homogeneous, so its elements are
        /// already contiguous key columns - convert them directly, without `membersFromArray` cutting
        /// each element into a size-1 column. Everything else keeps the general per-element path.
        const auto & nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
        if (auto fast = tryBuildFromNativeArray(rhs_column, nested_type, lhs_unpacked_types, params))
            return std::move(*fast);
        return createBlockFromCollection(membersFromArray(rhs_column, type), lhs_unpacked_types, params);
    };

    auto build_from_tuple = [&](const DataTypePtr & type)
    {
        return createBlockFromCollection(membersFromTuple(rhs_column, type), lhs_unpacked_types, params);
    };

    auto build_from_single_value = [&](const DataTypePtr & type)
    {
        return createBlockFromCollection(membersFromSingleValue(rhs_column, type), lhs_unpacked_types, params);
    };

    auto append_set_elements = [](ColumnsWithTypeAndName & destination, const ColumnsWithTypeAndName & source)
    {
        chassert(destination.size() == source.size());

        for (size_t i = 0; i < destination.size(); ++i)
        {
            chassert(source[i].column);
            chassert(destination[i].column);

            if (source[i].column->empty())
                continue;

            if (destination[i].column->empty())
            {
                destination[i].column = source[i].column;
                continue;
            }

            MutableColumnPtr merged = IColumn::mutate(std::move(destination[i].column));
            merged->reserve(merged->size() + source[i].column->size());
            merged->insertRangeFrom(*source[i].column, 0, source[i].column->size());
            destination[i].column = std::move(merged);
        }
    };


    size_t lhs_type_depth = getCompoundTypeDepth(*lhs_expression_type);
    size_t rhs_type_depth = getCompoundTypeDepth(*rhs_type);

    /// CAST(NULL, `Nullable(Tuple(...))`) IN NULL
    if (lhs_type_depth == rhs_type_depth + 1)
    {
        if (rhs_column->isNullAt(0))
            return build_from_single_value(rhs_type);
    }
    else if (lhs_type_depth == rhs_type_depth)
    {
        WhichDataType rhs_which_type(rhs_type);

        bool is_null_in_rhs = columnCollectionHasNull(rhs_column, rhs_which_type);
        bool has_tuple_in_rhs = columnCollectionHasTuple(rhs_column, rhs_type, rhs_which_type);

        if (lhs_is_tuple && rhs_which_type.isArray() && is_null_in_rhs)
        {
            /// CAST(NULL, `Nullable(Tuple(...))`) IN [NULL, NULL, (...)]
            /// Tuple(...) IN [NULL, NULL, (...)]
            return build_from_array(rhs_type);
        }

        if (lhs_is_tuple && rhs_which_type.isTuple() && is_null_in_rhs && (lhs_is_nullable || has_tuple_in_rhs))
        {
            /// RHS tuple can represent either:
            /// - a set of elements (NULLs and/or tuples): (NULL, NULL, (1, 2), ...)
            /// - a tuple literal (not a set): (NULL, 42) for `Tuple(Nullable(...), ...)`
            ///
            /// If RHS contains a non-null non-tuple element, it cannot be a set of tuples.
            ColumnPtr rhs_full = rhs_column->convertToFullColumnIfConst();
            const auto & rhs_tuple = assert_cast<const ColumnTuple &>(*rhs_full);
            const auto & rhs_tuple_element_types = assert_cast<const DataTypeTuple &>(*rhs_type).getElements();

            bool rhs_tuple_all_null = true;
            bool rhs_tuple_has_non_null_non_tuple = false;
            for (size_t i = 0, size = rhs_tuple.tupleSize(); i < size; ++i)
            {
                if (rhs_tuple.getColumn(i).isNullAt(0))
                    continue;

                rhs_tuple_all_null = false;

                if (getTupleType(rhs_tuple_element_types[i]) == nullptr)
                {
                    rhs_tuple_has_non_null_non_tuple = true;
                    break;
                }
            }

            /// Treat as a set of elements:
            /// - Tuple(...) IN (NULL, (1, 2), (3, 4))
            /// - CAST(NULL, `Nullable(Tuple(...))`) IN (NULL, NULL, (1, 2), ...)
            if (!rhs_tuple_has_non_null_non_tuple)
            {
                auto res = build_from_tuple(rhs_type);

                /// Additionally, for `Nullable(Tuple(...)) IN (NULL, NULL)` also treat RHS as a tuple literal `(NULL, NULL)`
                /// when it can be cast to the LHS tuple type (so both interpretations are supported).
                if (lhs_is_nullable && rhs_tuple_all_null && rhs_tuple.tupleSize() == lhs_tuple_type->getElements().size())
                {
                    /// Tuple literal `(NULL, NULL)` is representable only if all tuple elements are Nullable,
                    /// otherwise it would require NULL -> non-nullable conversion (e.g. `Nullable(Tuple(Int64, Int64))`).
                    bool all_tuple_elements_nullable = params.transform_null_in;
                    for (const auto & element_type : lhs_tuple_type->getElements())
                    {
                        if (!element_type->isNullable())
                        {
                            all_tuple_elements_nullable = false;
                            break;
                        }
                    }

                    if (all_tuple_elements_nullable)
                        append_set_elements(res, build_from_single_value(rhs_type));
                }

                return res;
            }
        }

        /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
        return build_from_single_value(rhs_type);
    }
    else if (lhs_type_depth + 1 == rhs_type_depth)
    {
        /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4))
        WhichDataType rhs_which_type(rhs_type);

        if (rhs_which_type.isArray())
            return build_from_array(rhs_type);

        if (rhs_which_type.isTuple())
            return build_from_tuple(rhs_type);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported type at the right-side of IN. Expected Array or Tuple or Nullable(Tuple). Actual {}",
            rhs_type->getName());
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Unsupported types for IN. First argument type {}. Second argument type {}",
        lhs_expression_type->getName(),
        rhs_type->getName());
}

}
