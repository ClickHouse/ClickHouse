#include <Interpreters/getDateTime64CastLossMap.h>
#include <Interpreters/castColumn.h>
#include <Functions/CastOverloadResolver.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/NullableUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsCommon.h>
#include <Core/DecimalFunctions.h>
#include <Core/callOnTypeIndex.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>


namespace DB
{

/// Marks the timestamps that `FloatType` does not hold exactly. Of all the timestamps that the cast rounds to
/// one floating-point value, only the one that the value converts back to stays unmarked.
template <typename FloatType>
static ColumnPtr getDateTime64ToFloatLossMap(const ColumnDecimal<DateTime64> & column)
{
    const auto & values = column.getData();
    auto result = ColumnUInt8::create(values.size(), UInt8(0));
    auto & loss_map = result->getData();
    for (size_t row = 0; row < values.size(); ++row)
        loss_map[row] = !DecimalUtils::convertsToFloatExactly<FloatType>(values[row], column.getScale());
    return result;
}

/// Marks the timestamps of the `DateTime64` column `source` that do not come back unchanged from a cast to
/// `to_type` and back, including those that either cast rejects. The internal casts drop the fraction below
/// the scale of their type, the time of day for a date and the date for a time, and saturate a value outside
/// the range of their type.
static ColumnPtr getDateTime64RoundTripLossMap(
    const ColumnWithTypeAndName & source, const DataTypePtr & to_type, InternalCastFunctionCache * cache)
{
    const auto converted = castColumnAccurateOrNull(source, to_type, cache);
    const auto & converted_nullable = assert_cast<const ColumnNullable &>(*converted);
    const auto restored = castColumnAccurateOrNull(
        {converted_nullable.getNestedColumnPtr(), to_type, source.name}, source.type, cache);
    const auto & restored_nullable = assert_cast<const ColumnNullable &>(*restored);

    const auto & values = assert_cast<const ColumnDecimal<DateTime64> &>(*source.column).getData();
    const auto & restored_values = assert_cast<const ColumnDecimal<DateTime64> &>(restored_nullable.getNestedColumn()).getData();
    const auto & not_converted = converted_nullable.getNullMapData();
    const auto & not_restored = restored_nullable.getNullMapData();
    auto result = ColumnUInt8::create(values.size());
    auto & loss_map = result->getData();
    for (size_t row = 0; row < values.size(); ++row)
        loss_map[row] = not_converted[row] || not_restored[row] || restored_values[row].value != values[row].value;
    return result;
}

/// Marks the timestamps of the `DateTime64` column `source` that a cast to the non-composite type `to_type`
/// changes. A timestamp matches a value of `to_type` only when the value converts back to it, as in a
/// comparison of the two. The cast back from a floating-point type truncates, so a float is checked by
/// rounding instead. Any other type that `DateTime64` converts to, like `String`, holds every timestamp
/// exactly and needs no check.
static ColumnPtr getDateTime64ToScalarLossMap(
    const ColumnWithTypeAndName & source, const DataTypePtr & to_type, InternalCastFunctionCache * cache)
{
    ColumnPtr float_loss_map;
    if (callOnBasicType<void, false, true, false, false>(to_type->getTypeId(), [&](const auto & types)
        {
            using FloatType = typename std::decay_t<decltype(types)>::RightType;
            float_loss_map = getDateTime64ToFloatLossMap<FloatType>(assert_cast<const ColumnDecimal<DateTime64> &>(*source.column));
            return true;
        }))
        return float_loss_map;

    const WhichDataType target(to_type);
    if (target.isIntegerOrDecimal() || target.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64())
        return getDateTime64RoundTripLossMap(source, to_type, cache);

    return {};
}

ColumnPtr getDateTime64CastLossMap(
    const ColumnWithTypeAndName & source, const DataTypePtr & target_type, InternalCastFunctionCache * cache)
{
    if (source.type->equals(*target_type))
        return {};

    const auto from_type = removeLowCardinality(source.type);
    const auto to_type = removeLowCardinalityAndNullable(target_type);
    const auto column = source.column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (from_type->isNullable())
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(*column);
        /// An all-NULL source has no timestamp to check and needs no structural conversion.
        if (countBytesInFilter(nullable.getNullMapData()) == nullable.size())
            return {};

        auto loss_map = getDateTime64CastLossMap(
            {nullable.getNestedColumnPtr(), removeNullable(from_type), source.name}, to_type, cache);
        if (!loss_map)
            return {};

        auto result = IColumn::mutate(std::move(loss_map));
        auto & values = assert_cast<ColumnUInt8 &>(*result).getData();
        const auto & nulls = nullable.getNullMapData();
        for (size_t row = 0; row < values.size(); ++row)
            values[row] &= !nulls[row];
        return result;
    }

    if (const auto * from_variant = typeid_cast<const DataTypeVariant *>(from_type.get()))
    {
        /// The cast converts every variant on its own and takes each row from its active variant, as
        /// `createVariantToColumnWrapper` does, so the loss maps of the variants are read back through the
        /// discriminators. A NULL row has no variant, and a variant without rows has no timestamp to check,
        /// like an all-NULL `Nullable` column above.
        const auto & column_variant = assert_cast<const ColumnVariant &>(*column);
        const auto & variant_types = from_variant->getVariants();

        Columns variant_loss_maps(variant_types.size());
        for (size_t global_discriminator = 0; global_discriminator < variant_types.size(); ++global_discriminator)
        {
            const auto & variant = column_variant.getVariantPtrByGlobalDiscriminator(global_discriminator);
            if (!variant->empty())
                variant_loss_maps[global_discriminator] = getDateTime64CastLossMap(
                    {variant, variant_types[global_discriminator], source.name}, to_type, cache);
        }
        if (std::ranges::none_of(variant_loss_maps, [](const ColumnPtr & loss_map) { return loss_map != nullptr; }))
            return {};

        auto result = ColumnUInt8::create(column_variant.size(), UInt8(0));
        auto & loss_map = result->getData();
        for (size_t row = 0; row < column_variant.size(); ++row)
        {
            const auto global_discriminator = column_variant.globalDiscriminatorAt(row);
            if (global_discriminator == ColumnVariant::NULL_DISCRIMINATOR || !variant_loss_maps[global_discriminator])
                continue;

            const auto & variant_loss_map = assert_cast<const ColumnUInt8 &>(*variant_loss_maps[global_discriminator]).getData();
            loss_map[row] = variant_loss_map[column_variant.offsetAt(row)];
        }
        return result;
    }

    if (const auto * from_map = typeid_cast<const DataTypeMap *>(from_type.get()))
        return getDateTime64CastLossMap(
            {assert_cast<const ColumnMap &>(*column).getNestedColumnPtr(), from_map->getNestedTypeWithUnnamedTuple(), source.name},
            to_type, cache);

    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to_type.get()))
    {
        if (const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from_type.get()))
        {
            /// A tuple-to-map cast converts the two arrays positionally into keys and values.
            chassert(from_tuple->getElements().size() == 2);
            const auto & elements = assert_cast<const ColumnTuple &>(*column).getColumns();
            ColumnPtr result;
            const auto & target_elements = to_map->getKeyValueTypes();
            for (size_t i = 0; i < 2; ++i)
                result = mergeNullMaps(std::move(result), getDateTime64CastLossMap(
                    {elements[i], from_tuple->getElements()[i], source.name}, std::make_shared<DataTypeArray>(target_elements[i]), cache));
            return result;
        }
        return getDateTime64CastLossMap({column, from_type, source.name}, to_map->getNestedTypeWithUnnamedTuple(), cache);
    }

    if (const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from_type.get()))
    {
        if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to_type.get()))
        {
            const auto positions = getTupleCastElementPositions(*from_tuple, *to_tuple);
            const auto & elements = assert_cast<const ColumnTuple &>(*column).getColumns();
            ColumnPtr result;
            for (size_t i = 0; i < positions.size(); ++i)
            {
                if (positions[i])
                {
                    const size_t from_index = *positions[i];
                    result = mergeNullMaps(std::move(result), getDateTime64CastLossMap(
                        {elements[from_index], from_tuple->getElements()[from_index], source.name}, to_tuple->getElements()[i], cache));
                }
            }
            return result;
        }
    }

    if (const auto * from_array = typeid_cast<const DataTypeArray *>(from_type.get()))
    {
        if (const auto * to_array = typeid_cast<const DataTypeArray *>(to_type.get()))
        {
            const auto & array = assert_cast<const ColumnArray &>(*column);
            auto element_loss = getDateTime64CastLossMap(
                {array.getDataPtr(), from_array->getNestedType(), source.name}, to_array->getNestedType(), cache);
            if (!element_loss)
                return {};

            const auto & values = assert_cast<const ColumnUInt8 &>(*element_loss).getData();
            const auto & offsets = array.getOffsets();
            auto result = ColumnUInt8::create(array.size(), UInt8(0));
            auto & rows = result->getData();
            for (size_t row = 0; row < rows.size(); ++row)
                for (size_t element = offsets[row - 1]; element < offsets[row]; ++element)
                    rows[row] |= values[element];
            return result;
        }
    }

    if (!typeid_cast<const DataTypeDateTime64 *>(from_type.get()))
        return {};

    return getDateTime64ToScalarLossMap({column, from_type, source.name}, to_type, cache);
}

}
