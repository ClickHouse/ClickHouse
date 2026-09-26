#include <Interpreters/convertColumnToType.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/castColumn.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
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
/// alternative with `resolveActiveAlternativeType` and pass its type here. Other tag-sensitive types
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

/// Whether a value of `type` occupies one of SEVERAL `Variant` alternatives, at any nesting depth. A
/// single-alternative `Variant` has no choice to lose, so it is not this class.
bool carriesAmbiguousVariant(const IDataType & type)
{
    bool result = false;
    auto check = [&](const IDataType & nested)
    {
        if (const auto * variant = typeid_cast<const DataTypeVariant *>(&nested))
            result |= variant->getVariants().size() > 1;
    };
    check(type);
    type.forEachChild(check);
    return result;
}

/// `CAST` resolves a `Variant` target's alternatives BY NAME, so it can choose the alternative for an
/// identity conversion, for a source type that names one, and for a `Variant` source whose alternatives
/// the target all names. By name, not `equals`: `equals` conflates types the lookup does not (a `DateTime`
/// timezone, an `AggregateFunction`'s serialization version). An ordinary `LowCardinality` survives the
/// source normalization, since it can be an alternative itself. Two tuples that both name their elements
/// are paired by NAME by `CAST` and positionally by `convertFieldToType`, so they qualify only where the
/// two pairings coincide.
bool variantAlternativeIsChosenByType(const DataTypePtr & from, const DataTypePtr & to)
{
    if (from->getName() == to->getName())
        return true;

    /// The lookup below ignores a source `Nullable`, while `CAST` cannot place a NULL in a target that
    /// does not hold one, so by type alone such a pair is refused and keeps the `Field` path, which owns
    /// the "not representable" answer for it. A `Variant` does hold a NULL, through its own discriminator.
    if (isNullableOrLowCardinalityNullable(from) && !canContainNull(*to))
        return false;

    const DataTypePtr source = removeNullableOrLowCardinalityNullable(from);

    /// A `Nullable` target adds a NULL flag over a conversion `CAST` performs unchanged, so the nested
    /// type answers the same question. Only a composite is reached: a `Variant` cannot be inside `Nullable`.
    if (const auto * to_nullable = typeid_cast<const DataTypeNullable *>(to.get()))
        return variantAlternativeIsChosenByType(source, to_nullable->getNestedType());

    if (const auto * to_variant = typeid_cast<const DataTypeVariant *>(to.get()))
    {
        if (const auto * from_variant = typeid_cast<const DataTypeVariant *>(source.get()))
        {
            for (const auto & alternative : from_variant->getVariants())
                if (!to_variant->tryGetVariantDiscriminator(alternative->getName()))
                    return false;
            return true;
        }

        return to_variant->tryGetVariantDiscriminator(source->getName()).has_value();
    }

    if (const auto * to_array = typeid_cast<const DataTypeArray *>(to.get()))
    {
        const auto * from_array = typeid_cast<const DataTypeArray *>(source.get());
        return from_array && variantAlternativeIsChosenByType(from_array->getNestedType(), to_array->getNestedType());
    }

    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        const auto * from_map = typeid_cast<const DataTypeMap *>(source.get());
        return from_map && variantAlternativeIsChosenByType(from_map->getKeyType(), to_map->getKeyType())
            && variantAlternativeIsChosenByType(from_map->getValueType(), to_map->getValueType());
    }

    if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get()))
    {
        const auto * from_tuple = typeid_cast<const DataTypeTuple *>(source.get());
        if (!from_tuple || from_tuple->getElements().size() != to_tuple->getElements().size())
            return false;

        if (from_tuple->hasExplicitNames() && to_tuple->hasExplicitNames()
            && from_tuple->getElementNames() != to_tuple->getElementNames())
            return false;

        for (size_t i = 0; i < to_tuple->getElements().size(); ++i)
            if (!variantAlternativeIsChosenByType(from_tuple->getElements()[i], to_tuple->getElements()[i]))
                return false;
        return true;
    }

    return false;
}

/// A `Field` cannot express a `Variant` result: `convertFieldToType` returns the value unchanged, and the
/// alternative is then chosen on insertion into a `ColumnVariant`, by the first one that accepts it (so
/// `1 :: UInt64` lands in `Date` for `Variant(Date, UInt64)`). `CAST` chooses it by type.
std::optional<ColumnPtr> tryConvertVariantColumnNative(
    const IColumn & value, const DataTypePtr & from, const DataTypePtr & to)
{
    if (!carriesAmbiguousVariant(*to))
        return std::nullopt;

    /// A `Nullable` that this row does not use holds no NULL for `CAST` to place, so the outermost one is
    /// decided by the row; the levels below have no value here and keep the type-level answer.
    const DataTypePtr source = value.isNullAt(0) ? from : removeNullableOrLowCardinalityNullable(from);

    if (!variantAlternativeIsChosenByType(source, to))
        return std::nullopt;

    /// The value keeps its own type here, so it stays representable, and neither `strict` nor
    /// `convert_inexact_floats` applies.
    return castColumn({value.getPtr(), from, ""}, to)->convertToFullColumnIfConst();
}

/// The type of the alternative that row 0 of a `Variant`/`Dynamic` column occupies, else `from`. A
/// genuine value of such a constant has that type rather than the carrier's - the carrier only records
/// which alternative it is - and `convertFieldToType` keys its conversions on the source type (an `Enum`
/// renders as its name, a `Bool` as `'true'`/`'false'`), while `IColumn::get` returns the nested value
/// alone. One layer is enough: an alternative may not itself be a `Variant` or a `Dynamic`. A `NULL` row
/// has no alternative, and the callers own the `NULL` contract.
DataTypePtr resolveActiveAlternativeType(const IColumn & value, const DataTypePtr & from)
{
    if (const auto * dynamic_column = checkAndGetColumn<ColumnDynamic>(&value))
    {
        if (DataTypePtr alternative = dynamic_column->getTypeAt(0))
            return alternative;
    }
    else if (const auto * variant_column = checkAndGetColumn<ColumnVariant>(&value))
    {
        /// `ColumnVariant` does not store the alternatives' types, so they are named by `from`. The bound
        /// also rejects `NULL_DISCRIMINATOR`, which is above every valid alternative index.
        const auto discriminator = variant_column->globalDiscriminatorAt(0);
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(from.get());
        if (variant_type && discriminator < variant_type->getVariants().size())
            return variant_type->getVariant(discriminator);
    }

    return from;
}

}

bool fieldCanLoseVariantAlternative(const DataTypePtr & from, const DataTypePtr & to)
{
    return carriesAmbiguousVariant(*from) || carriesAmbiguousVariant(*to) || from->hasDynamicStructure()
        || to->hasDynamicStructure();
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

    if (auto variant = tryConvertVariantColumnNative(unwrapped, from, to))
        return std::move(*variant);

    /// Fallback: materialize a `Field`, reuse `convertFieldToType`, rebuild a column. Column-native
    /// fast paths above shrink this over time; the differential test pins equivalence.
    Field field;
    unwrapped.get(0, field);
    /// `get` keeps neither the `Bool` tag nor the active `Variant`/`Dynamic` alternative; restore both so
    /// the delegated `convertFieldToType` behaves as it would for a genuine value of the constant.
    const DataTypePtr source = resolveActiveAlternativeType(unwrapped, from);
    retagBoolInField(field, source);

    const Field converted = convertFieldToType(field, *to, source.get(), format_settings, strict, convert_inexact_floats);

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
        Field field;
        full->get(0, field);
        const DataTypePtr source = resolveActiveAlternativeType(*full, from);
        retagBoolInField(field, source);
        convertFieldToTypeOrThrow(field, *to, source.get(), format_settings, convert_inexact_floats);

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
