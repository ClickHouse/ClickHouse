#include <Interpreters/convertColumnToType.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/castColumn.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TYPE_MISMATCH;
}

namespace
{

/// Column-native conversion for the cases where CAST provably matches `convertFieldToType`:
/// plain native-numeric-to-native-numeric, in the default AND `strict` modes. `castColumnAccurateOrNull`
/// uses the same accurate numeric conversion (`accurate::convertNumeric`) as `convertFieldToType`'s
/// default path (out-of-range / inexact-narrowing -> NULL). For native numbers `strict` and default
/// agree with it too: `strict` only rejects additional cases for types that are already excluded here -
/// `Bool` (10 is not a valid `Bool`) and `Decimal` (scale reduction; `castColumnAccurateOrNull` would
/// round, so it must NOT be used for strict Decimal - and it isn't, `Decimal` is not a native number).
/// The strict native-number equivalence is pinned by `gtest_convert_column_to_type`. Returns:
///   - the converted size-1 column of `to` on success,
///   - a null `ColumnPtr{}` when not representable,
///   - std::nullopt when this fast path does not apply (caller falls back to the `Field` path).
/// Excluded: `convert_inexact_floats` mode (allows rounding, so `castColumnAccurateOrNull` differs),
/// `Bool` (clamp/validity semantics), and anything non-native-numeric
/// (Decimal/Date/Enum/String/wide-int/wrappers/composite).
std::optional<ColumnPtr> tryConvertNumericColumnNative(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    bool convert_inexact_floats)
{
    if (convert_inexact_floats)
        return std::nullopt;
    if (!isNativeNumber(from) || !isNativeNumber(to) || isBool(from) || isBool(to))
        return std::nullopt;
    /// `strict` is intentionally not a parameter: for native numbers it is equivalent to the default
    /// accurate path (both reject non-representable values), so this fast path serves strict too.

    ColumnWithTypeAndName arg{value.getPtr(), from, ""};
    ColumnPtr casted = castColumnAccurateOrNull(arg, to);
    /// `ExecutableFunctionCast` uses the default implementation for constants, so a `ColumnConst`
    /// argument yields a `ColumnConst` result. Callers already pass a full column, but unwrap here too
    /// so the `assert_cast` below is correct regardless of the argument's constness.
    casted = casted->convertToFullColumnIfConst();
    const auto & nullable = assert_cast<const ColumnNullable &>(*casted);
    if (nullable.isNullAt(0))
        return ColumnPtr{};
    return nullable.getNestedColumnPtr();
}

/// `IColumn::get` reconstructs a `Field` using the storage column's `NearestFieldType`, which does not
/// round-trip the `Field` tag for `Bool`: a `DataTypeBool` column is a plain `ColumnUInt8`, so `get`
/// yields a `UInt64` `Field`. `convertFieldToType` keys on that tag (e.g. `Bool -> String` gives
/// 'true'/'false' for a `Bool` field but '1'/'0' for a `UInt64` one), so re-tag `Bool` values in the
/// reconstructed field - recursing through `Array`/`Tuple`/`Map` and unwrapping `Nullable`/
/// `LowCardinality` - so the delegated `convertFieldToType` sees what it would for a genuine value of
/// `from`. Verified by `gtest_convert_column_to_type`.
///
/// A `Bool` under `Variant`/`Dynamic` is reached as well, because the callers resolve the active
/// alternative with `resolveActiveAlternative` and pass its type here. Other tag-sensitive types
/// (IPv4/IPv6/UUID/Decimal) have dedicated columns/`Field` types and round-trip through `get` already.
void retagBoolInField(Field & field, const DataTypePtr & type)
{
    if (field.isNull())
        return;

    const DataTypePtr unwrapped = removeLowCardinalityAndNullable(type);

    if (isBool(unwrapped))
    {
        field = Field(field.safeGet<UInt64>() != 0);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(unwrapped.get()))
    {
        for (auto & element : field.safeGet<Array>())
            retagBoolInField(element, array_type->getNestedType());
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(unwrapped.get()))
    {
        auto & tuple = field.safeGet<Tuple>();
        const auto & element_types = tuple_type->getElements();
        for (size_t i = 0; i < tuple.size() && i < element_types.size(); ++i)
            retagBoolInField(tuple[i], element_types[i]);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(unwrapped.get()))
    {
        for (auto & entry : field.safeGet<Map>())
        {
            auto & key_and_value = entry.safeGet<Tuple>();
            if (key_and_value.size() == 2)
            {
                retagBoolInField(key_and_value[0], map_type->getKeyType());
                retagBoolInField(key_and_value[1], map_type->getValueType());
            }
        }
        return;
    }
}

/// The alternative that row `row` of a `Variant`/`Dynamic` column occupies: its type, and the column and
/// row that hold the value. A genuine value of such a constant has that type rather than the carrier's -
/// the carrier only records which alternative it is - and `convertFieldToType` keys its conversions on the
/// source type (an `Enum` renders as its name, a `Bool` as `'true'`/`'false'`), while `IColumn::get`
/// returns the nested value alone. One layer is enough: an alternative may not itself be a `Variant` or a
/// `Dynamic`, although it may be a composite that contains one. For any other column, and for a `NULL`
/// row, which has no alternative, this is `from`, `value` and `row` themselves.
struct ActiveAlternative
{
    DataTypePtr type;
    const IColumn * column = nullptr;
    size_t row = 0;
    /// Owns `column` when it was deserialized from the shared variant of a `Dynamic`.
    ColumnPtr holder;
};

ActiveAlternative resolveActiveAlternative(const IColumn & value, const DataTypePtr & from, size_t row)
{
    if (const auto * dynamic_column = checkAndGetColumn<ColumnDynamic>(&value))
    {
        const auto & variant_column = dynamic_column->getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
            return {from, &value, row, {}};

        if (discriminator == dynamic_column->getSharedVariantDiscriminator())
        {
            /// A value of a type beyond `max_types` is stored in the shared variant in binary, after its type.
            const auto data = dynamic_column->getSharedVariant().getDataAt(variant_column.offsetAt(row));
            ReadBufferFromMemory buf(data);
            DataTypePtr type = decodeDataType(buf);
            auto column = type->createColumn();
            type->getDefaultSerialization()->deserializeBinary(*column, buf, FormatSettings{});
            ColumnPtr holder = std::move(column);
            return {std::move(type), holder.get(), 0, holder};
        }

        const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_column->getVariantInfo().variant_type);
        return {
            variant_type.getVariant(discriminator),
            &variant_column.getVariantByGlobalDiscriminator(discriminator),
            variant_column.offsetAt(row),
            {}};
    }

    if (const auto * variant_column = checkAndGetColumn<ColumnVariant>(&value))
    {
        /// `ColumnVariant` does not store the alternatives' types, so they are named by `from`. The bound
        /// also rejects `NULL_DISCRIMINATOR`, which is above every valid alternative index.
        const auto discriminator = variant_column->globalDiscriminatorAt(row);
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(from.get());
        if (variant_type && discriminator < variant_type->getVariants().size())
            return {
                variant_type->getVariant(discriminator),
                &variant_column->getVariantByGlobalDiscriminator(discriminator),
                variant_column->offsetAt(row),
                {}};
    }

    return {from, &value, row, {}};
}

bool isVariantOrDynamic(const IDataType & type)
{
    return typeid_cast<const DataTypeVariant *>(&type) || isDynamic(type);
}

bool containsVariantOrDynamic(const DataTypePtr & type)
{
    if (isVariantOrDynamic(*type))
        return true;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return containsVariantOrDynamic(nullable_type->getNestedType());
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return containsVariantOrDynamic(array_type->getNestedType());
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return std::ranges::any_of(tuple_type->getElements(), [](const DataTypePtr & element) { return containsVariantOrDynamic(element); });
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
        return containsVariantOrDynamic(map_type->getKeyType()) || containsVariantOrDynamic(map_type->getValueType());
    return false;
}

/// A composite holding a `Variant`/`Dynamic` converted to a `String` as a whole is rendered from its column:
/// its `Field` keeps none of the members' alternatives, so it cannot be rendered as `CAST` renders it.
bool isCompositeWithAlternativesToString(const DataTypePtr & from, const DataTypePtr & unwrapped_to)
{
    return (isArray(from) || isTuple(from) || isMap(from)) && isStringOrFixedString(unwrapped_to) && containsVariantOrDynamic(from);
}

/// `resolveActiveAlternative` recovers the alternative of a top-level `Variant`/`Dynamic` only. Inside a
/// composite (`Array(Dynamic)`, `Tuple(Variant(...))`, `Map(K, Dynamic)`) every member may occupy a
/// different alternative, so no single source type describes them and `convertFieldToType` would see just
/// the carrier - a `Bool` member then renders as `'1'` instead of what `CAST` writes. So convert each such
/// member here, with its own alternative as the source type, straight to the matching member of `to`.
/// `rewriteNestedAlternativesType` gives the source type of the result: those members already have the
/// type of `to`, and `convertFieldToType` leaves a value whose source type equals the target as it is. A
/// composite that holds such members and is converted to a `String` as a whole is rendered here as well.
/// `from` is never a `Variant`/`Dynamic` itself at the top level: the callers resolve that alternative first.
DataTypePtr rewriteNestedAlternativesType(const DataTypePtr & from, const DataTypePtr & to)
{
    if (isVariantOrDynamic(*from))
        return to;

    if (const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from.get()))
    {
        const DataTypePtr nested = rewriteNestedAlternativesType(nullable_from->getNestedType(), to);
        return nested == nullable_from->getNestedType() ? from : makeNullable(nested);
    }

    const DataTypePtr unwrapped_to = removeLowCardinalityAndNullable(to);

    if (isCompositeWithAlternativesToString(from, unwrapped_to))
        return std::make_shared<DataTypeString>();

    if (const auto * array_from = typeid_cast<const DataTypeArray *>(from.get()))
    {
        const auto * array_to = typeid_cast<const DataTypeArray *>(unwrapped_to.get());
        if (!array_to)
            return from;
        const DataTypePtr nested = rewriteNestedAlternativesType(array_from->getNestedType(), array_to->getNestedType());
        return nested == array_from->getNestedType() ? from : std::make_shared<DataTypeArray>(nested);
    }

    if (const auto * tuple_from = typeid_cast<const DataTypeTuple *>(from.get()))
    {
        const auto * tuple_to = typeid_cast<const DataTypeTuple *>(unwrapped_to.get());
        if (!tuple_to || tuple_to->getElements().size() != tuple_from->getElements().size())
            return from;
        DataTypes elements = tuple_from->getElements();
        bool changed = false;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            DataTypePtr element = rewriteNestedAlternativesType(elements[i], tuple_to->getElements()[i]);
            changed |= element != elements[i];
            elements[i] = std::move(element);
        }
        if (!changed)
            return from;
        if (tuple_from->hasExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple_from->getElementNames());
        return std::make_shared<DataTypeTuple>(elements);
    }

    if (const auto * map_from = typeid_cast<const DataTypeMap *>(from.get()))
    {
        const auto * map_to = typeid_cast<const DataTypeMap *>(unwrapped_to.get());
        if (!map_to)
            return from;
        const DataTypePtr key = rewriteNestedAlternativesType(map_from->getKeyType(), map_to->getKeyType());
        const DataTypePtr value = rewriteNestedAlternativesType(map_from->getValueType(), map_to->getValueType());
        if (key == map_from->getKeyType() && value == map_from->getValueType())
            return from;
        return std::make_shared<DataTypeMap>(key, value);
    }

    return from;
}

/// Converts the members of `field` that `rewriteNestedAlternativesType` retypes, walking row `row` of `column`
/// alongside it to find the alternative of each member. `field` was read from that row, so its shape matches.
/// Returns false when a member cannot be represented in its target, which a non-`Nullable` target cannot hold
/// as `NULL` either: then, as in `convertFieldToType`, the whole value cannot be represented.
bool rewriteNestedAlternatives(
    Field & field,
    const IColumn & column,
    size_t row,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats);

/// Converts `field`, read from row `row` of `column` of type `from`, which may be a `Variant`/`Dynamic`,
/// to `to`. Returns false if it cannot be represented.
bool convertAlternative(
    Field & field,
    const IColumn & column,
    size_t row,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    if (!field.isNull())
    {
        const ActiveAlternative alternative = resolveActiveAlternative(column, from, row);
        retagBoolInField(field, alternative.type);
        /// The alternative can be a composite holding `Variant`/`Dynamic` members of its own.
        const DataTypePtr source = rewriteNestedAlternativesType(alternative.type, to);
        if (source != alternative.type
            && !rewriteNestedAlternatives(
                field, *alternative.column, alternative.row, alternative.type, to, format_settings, strict, convert_inexact_floats))
            return false;
        field = convertFieldToType(field, *to, source.get(), format_settings, strict, convert_inexact_floats);
    }
    return !field.isNull() || canContainNull(*to);
}

bool rewriteNestedAlternatives(
    Field & field,
    const IColumn & column,
    size_t row,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    if (isVariantOrDynamic(*from))
        return convertAlternative(field, column, row, from, to, format_settings, strict, convert_inexact_floats);

    /// A `NULL` of a `Nullable` member is left to `convertFieldToType`, as its type is not retyped to `to`.
    if (field.isNull())
        return true;

    if (const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from.get()))
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(column);
        return rewriteNestedAlternatives(
            field, nullable_column.getNestedColumn(), row, nullable_from->getNestedType(), to,
            format_settings, strict, convert_inexact_floats);
    }

    const DataTypePtr unwrapped_to = removeLowCardinalityAndNullable(to);

    if (isCompositeWithAlternativesToString(from, unwrapped_to))
    {
        WriteBufferFromOwnString out;
        from->getDefaultSerialization()->serializeText(column, row, out, format_settings);
        field = out.str();
        return true;
    }

    if (const auto * array_from = typeid_cast<const DataTypeArray *>(from.get()))
    {
        const auto * array_to = typeid_cast<const DataTypeArray *>(unwrapped_to.get());
        if (!array_to)
            return true;
        const auto & array_column = assert_cast<const ColumnArray &>(column);
        const size_t offset = row == 0 ? 0 : array_column.getOffsets()[row - 1];
        auto & elements = field.safeGet<Array>();
        for (size_t i = 0; i < elements.size(); ++i)
            if (!rewriteNestedAlternatives(
                    elements[i], array_column.getData(), offset + i, array_from->getNestedType(), array_to->getNestedType(),
                    format_settings, strict, convert_inexact_floats))
                return false;
        return true;
    }

    if (const auto * tuple_from = typeid_cast<const DataTypeTuple *>(from.get()))
    {
        const auto * tuple_to = typeid_cast<const DataTypeTuple *>(unwrapped_to.get());
        if (!tuple_to || tuple_to->getElements().size() != tuple_from->getElements().size())
            return true;
        const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
        auto & elements = field.safeGet<Tuple>();
        for (size_t i = 0; i < elements.size(); ++i)
            if (!rewriteNestedAlternatives(
                    elements[i], tuple_column.getColumn(i), row, tuple_from->getElements()[i], tuple_to->getElements()[i],
                    format_settings, strict, convert_inexact_floats))
                return false;
        return true;
    }

    if (const auto * map_from = typeid_cast<const DataTypeMap *>(from.get()))
    {
        const auto * map_to = typeid_cast<const DataTypeMap *>(unwrapped_to.get());
        if (!map_to)
            return true;
        const auto & nested_array = assert_cast<const ColumnMap &>(column).getNestedColumn();
        const auto & key_value_columns = assert_cast<const ColumnTuple &>(nested_array.getData());
        const size_t offset = row == 0 ? 0 : nested_array.getOffsets()[row - 1];
        auto & entries = field.safeGet<Map>();
        for (size_t i = 0; i < entries.size(); ++i)
        {
            auto & key_and_value = entries[i].safeGet<Tuple>();
            if (!rewriteNestedAlternatives(
                    key_and_value[0], key_value_columns.getColumn(0), offset + i, map_from->getKeyType(), map_to->getKeyType(),
                    format_settings, strict, convert_inexact_floats)
                || !rewriteNestedAlternatives(
                    key_and_value[1], key_value_columns.getColumn(1), offset + i, map_from->getValueType(), map_to->getValueType(),
                    format_settings, strict, convert_inexact_floats))
                return false;
        }
        return true;
    }

    return true;
}

/// Row 0 of `value` as the `Field` and the source type that the delegated `convertFieldToType` should see.
struct SourceField
{
    Field field;
    DataTypePtr type;
    /// False when a member of a composite cannot be represented in the matching member of `to`, so that
    /// the whole value cannot be represented either. Then `field` and `type` are the value as it was read.
    bool representable = true;
};

SourceField getSourceField(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    SourceField source;
    value.get(0, source.field);
    /// `get` keeps neither the `Bool` tag nor the active `Variant`/`Dynamic` alternative; restore both so
    /// the delegated `convertFieldToType` behaves as it would for a genuine value of the constant.
    const ActiveAlternative alternative = resolveActiveAlternative(value, from, 0);
    source.type = alternative.type;
    retagBoolInField(source.field, source.type);

    if (source.field.isNull())
        return source;

    const DataTypePtr rewritten_type = rewriteNestedAlternativesType(source.type, to);
    if (rewritten_type == source.type)
        return source;

    Field rewritten_field = source.field;
    if (!rewriteNestedAlternatives(
            rewritten_field, *alternative.column, alternative.row, source.type, to, format_settings, strict, convert_inexact_floats))
    {
        source.representable = false;
        return source;
    }

    source.field = std::move(rewritten_field);
    source.type = rewritten_type;
    return source;
}

}

ColumnPtr convertColumnToTypeOrNull(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    chassert(value.size() == 1);

    /// Callers usually pass a `ColumnConst` (e.g. from `evaluateConstantExpressionAsColumn`); operate
    /// on the underlying full column so the fast path's CAST returns a plain (non-const) column and the
    /// `Field` fallback reads the value directly.
    const ColumnPtr full = value.convertToFullColumnIfConst();
    const IColumn & unwrapped = *full;

    if (auto native = tryConvertNumericColumnNative(unwrapped, from, to, convert_inexact_floats))
        return std::move(*native);

    /// Fallback: materialize a `Field`, reuse `convertFieldToType`, rebuild a column. Column-native
    /// fast paths above shrink this over time; the differential test pins equivalence.
    const SourceField source = getSourceField(unwrapped, from, to, format_settings, strict, convert_inexact_floats);
    if (!source.representable)
        return {};
    const Field & field = source.field;

    const Field converted = convertFieldToType(field, *to, source.type.get(), format_settings, strict, convert_inexact_floats);

    if (converted.isNull())
    {
        /// `convertFieldToType` returns a Null `Field` for two different outcomes: a legitimate NULL
        /// result (NULL input into a type that can hold NULL) and "not representable". Distinguish
        /// them here instead of collapsing both into a null `ColumnPtr`: a valid NULL becomes a
        /// size-1 column holding NULL, while "not representable" is the null `ColumnPtr{}`.
        if (field.isNull() && canContainNull(*to))
        {
            auto null_column = to->createColumn();
            null_column->insert(Field());
            return null_column;
        }
        return {};
    }

    auto column = to->createColumn();
    column->insert(converted);
    return column;
}

ColumnPtr tryConvertColumnToTypeOrNull(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool strict,
    bool convert_inexact_floats)
{
    try
    {
        return convertColumnToTypeOrNull(value, from, to, format_settings, strict, convert_inexact_floats);
    }
    catch (...) // Ok: try-pattern that intentionally maps any conversion failure to a null result
    {
        return {};
    }
}

ColumnPtr convertColumnToTypeOrThrow(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings,
    bool convert_inexact_floats)
{
    chassert(value.size() == 1);

    /// Mirror `convertFieldToTypeOrThrow`: a NULL that the target cannot hold is a type mismatch,
    /// while a non-NULL value that does not fit the target is out of range.
    if (value.isNullAt(0) && !canContainNull(*to))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot convert NULL to {}", to->getName());

    ColumnPtr result = convertColumnToTypeOrNull(value, from, to, format_settings, /*strict=*/false, convert_inexact_floats);

    if (!value.isNullAt(0) && !result)
    {
        /// Reproduce `convertFieldToTypeOrThrow`'s diagnostic (which names the offending value and the
        /// types) instead of a generic message - materializing a `Field` only on this exceptional path;
        /// the happy path in `convertColumnToTypeOrNull` stays `Field`-free.
        const ColumnPtr full = value.convertToFullColumnIfConst();
        const SourceField source = getSourceField(*full, from, to, format_settings, /*strict=*/false, convert_inexact_floats);
        if (!source.representable)
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Cannot convert value '{}' from {}: it cannot be represented as {}",
                fieldToString(source.field),
                source.type->getName(),
                to->getName());
        convertFieldToTypeOrThrow(source.field, *to, source.type.get(), format_settings, convert_inexact_floats);

        /// `convertFieldToTypeOrThrow` must have thrown for a value that `convertColumnToTypeOrNull`
        /// reported as not representable; guard in case the two ever disagree.
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Value in column of type {} cannot be represented as {}",
            from->getName(),
            to->getName());
    }

    return result;
}

}
