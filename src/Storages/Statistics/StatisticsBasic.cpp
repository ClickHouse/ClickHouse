#include <Storages/Statistics/StatisticsBasic.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Serialized feature bits inside a `basic` statistics blob. Stored as a UInt8 right after
/// `row_count` so new sub-statistics can be added without bumping the global file version: a
/// reader that doesn't know about a bit simply skips the block (size is implied by the bits).
enum BasicFeatureMask : UInt8
{
    NumericMinMax = 1u << 0,
    StringLengthSum = 1u << 1,
    NullCount = 1u << 2,
};

UInt64 countNullsInColumn(const ColumnPtr & column)
{
    auto full = column->convertToFullColumnIfSparse();

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(full.get()))
    {
        const auto & null_map = nullable->getNullMapData();
        return std::count(null_map.begin(), null_map.end(), 1);
    }

    if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(full.get()))
    {
        if (!lc->nestedIsNullable())
            return 0;

        size_t null_index = lc->getDictionary().getNullValueIndex();
        const auto & indexes = lc->getIndexes();
        UInt64 cnt = 0;
        for (size_t i = 0, n = indexes.size(); i < n; ++i)
            if (indexes.getUInt(i) == null_index)
                ++cnt;
        return cnt;
    }

    return 0;
}

const NullMap * tryGetNullMap(const IColumn & column)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
        return &nullable->getNullMapData();
    return nullptr;
}

/// Sum byte lengths of *non-NULL* values in `column`. `LowCardinality` is expanded before
/// reading the null map so that `LowCardinality(Nullable(...))` rows are excluded correctly
/// (the null map lives on the inner `Nullable`, not the outer LC).
UInt64 sumNonNullStringBytes(const ColumnPtr & column)
{
    auto full = column->convertToFullColumnIfSparse();
    if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(full.get()))
        full = lc->convertToFullColumn();
    const NullMap * null_map = tryGetNullMap(*full);

    ColumnPtr values = full;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(values.get()))
        values = nullable->getNestedColumnPtr();

    const size_t column_size = column->size();
    if (const auto * fs = typeid_cast<const ColumnFixedString *>(values.get()))
    {
        UInt64 non_null = column_size;
        if (null_map)
            non_null -= std::count(null_map->begin(), null_map->end(), 1);
        return fs->getN() * non_null;
    }
    if (const auto * s = typeid_cast<const ColumnString *>(values.get()))
    {
        UInt64 total = 0;
        for (size_t i = 0; i < column_size; ++i)
        {
            if (null_map && (*null_map)[i])
                continue;
            total += s->getDataAt(i).size();
        }
        return total;
    }
    return 0;
}

}


StatisticsBasic::StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeLowCardinalityAndNullable(removeNullable(data_type_)))
{
    tracks_numeric = data_type->isValueRepresentedByNumber();
    tracks_string = isStringOrFixedString(data_type);
    tracks_null = isNullableOrLowCardinalityNullable(data_type_);
}

void StatisticsBasic::build(const ColumnPtr & column)
{
    const size_t column_size = column->size();

    if (tracks_null)
        null_count += countNullsInColumn(column);

    if (tracks_numeric)
    {
        /// `IColumn::getExtremes` ignores NULLs on a `ColumnNullable`, and Sparse is handled via
        /// the conversion in `convertToFullColumnIfSparse` inside `getExtremes`.
        Field min_field;
        Field max_field;
        column->getExtremes(min_field, max_field, 0, column_size);

        if (!min_field.isNull() && (min.isNull() || min_field < min))
            min = min_field;
        if (!max_field.isNull() && (max.isNull() || max_field > max))
            max = max_field;
    }

    if (tracks_string)
        string_total_bytes += sumNonNullStringBytes(column);

    row_count += column_size;
}

void StatisticsBasic::merge(const StatisticsPtr & other_stats)
{
    const auto * other = typeid_cast<const StatisticsBasic *>(other_stats.get());
    if (!other)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge Basic statistics with a different type");

    if (tracks_numeric)
    {
        if (!other->min.isNull() && (min.isNull() || other->min < min))
            min = other->min;
        if (!other->max.isNull() && (max.isNull() || other->max > max))
            max = other->max;
    }
    if (tracks_string)
        string_total_bytes += other->string_total_bytes;
    if (tracks_null)
        null_count += other->null_count;

    row_count += other->row_count;
}

void StatisticsBasic::serialize(WriteBuffer & buf)
{
    writeIntBinary(row_count, buf);

    UInt8 mask = 0;
    if (tracks_numeric)
        mask |= BasicFeatureMask::NumericMinMax;
    if (tracks_string)
        mask |= BasicFeatureMask::StringLengthSum;
    if (tracks_null)
        mask |= BasicFeatureMask::NullCount;
    writeIntBinary(mask, buf);

    if (tracks_numeric)
    {
        writeStringBinary(data_type->getName(), buf);
        writeFieldBinary(min, buf);
        writeFieldBinary(max, buf);
    }
    if (tracks_string)
        writeIntBinary(string_total_bytes, buf);
    if (tracks_null)
        writeIntBinary(null_count, buf);
}

void StatisticsBasic::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    readIntBinary(row_count, buf);

    UInt8 mask = 0;
    readIntBinary(mask, buf);

    if (mask & BasicFeatureMask::NumericMinMax)
    {
        String stored_type_name;
        readStringBinary(stored_type_name, buf);
        min = readFieldBinary(buf);
        max = readFieldBinary(buf);

        if (tracks_numeric && stored_type_name != data_type->getName())
        {
            /// The column type has changed (e.g. via `MODIFY COLUMN`). Try to convert the stored
            /// bounds to the new type; on failure leave them as `Null` Fields, which the estimator
            /// treats as "not initialized" and falls back gracefully.
            auto stored_type = DataTypeFactory::instance().get(stored_type_name);
            if (!min.isNull())
            {
                Field converted = convertFieldToType(min, *data_type, stored_type.get());
                min = std::move(converted); /// `Null` on conversion failure — treated as "not initialized"
            }
            if (!max.isNull())
            {
                Field converted = convertFieldToType(max, *data_type, stored_type.get());
                max = std::move(converted);
            }
        }
        else if (!tracks_numeric)
        {
            /// The column was numeric when the stats were built but isn't now. Discard the bounds.
            min = Field();
            max = Field();
        }
    }

    if (mask & BasicFeatureMask::StringLengthSum)
    {
        readIntBinary(string_total_bytes, buf);
        if (!tracks_string)
            string_total_bytes = 0;
    }

    if (mask & BasicFeatureMask::NullCount)
    {
        readIntBinary(null_count, buf);
        if (!tracks_null)
            null_count = 0;
    }
}

std::optional<Float64> StatisticsBasic::estimateLess(const Field & val) const
{
    if (!tracks_numeric)
        return std::nullopt;
    if (row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    /// Total non-NULL rows known to the part: the linear-interpolation domain.
    const UInt64 non_null = (tracks_null && null_count <= row_count) ? (row_count - null_count) : row_count;
    if (non_null == 0)
        return 0.0;

    auto interpolate = [non_null](Float64 v, Float64 mn, Float64 mx) -> Float64
    {
        if (v < mn) return 0.0;
        if (v > mx) return static_cast<Float64>(non_null);
        if (mn == mx) return (v == mx) ? static_cast<Float64>(non_null) : 0.0;
        return (v - mn) / (mx - mn) * static_cast<Float64>(non_null);
    };

    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    auto min_as_float = StatisticsUtils::tryConvertToFloat64(min, data_type);
    auto max_as_float = StatisticsUtils::tryConvertToFloat64(max, data_type);
    if (!val_as_float || !min_as_float || !max_as_float)
        return std::nullopt;
    return interpolate(*val_as_float, *min_as_float, *max_as_float);
}

String StatisticsBasic::getNameForLogs() const
{
    String result = "Basic: ";
    bool first = true;
    auto sep = [&](const char * label)
    {
        if (!first)
            result += ", ";
        result += label;
        result += '=';
        first = false;
    };

    if (tracks_numeric)
    {
        sep("minmax");
        result += "(" + applyVisitor(FieldVisitorToString(), min) + ", " + applyVisitor(FieldVisitorToString(), max) + ")";
    }
    if (tracks_string)
    {
        sep("string_length_avg");
        result += std::to_string(getStringLengthAvg());
    }
    if (tracks_null)
    {
        sep("null_count");
        result += std::to_string(null_count);
    }
    if (first)
        result += "(empty)";
    return result;
}

Int64 StatisticsBasic::getStringLengthAvg() const
{
    /// Denominator = number of non-NULL rows we summed bytes over. When the column is Nullable
    /// we subtract `null_count`; otherwise every processed row contributed.
    const UInt64 non_null = (tracks_null && null_count <= row_count) ? (row_count - null_count) : row_count;
    if (non_null == 0)
        return 0;
    return static_cast<Int64>(string_total_bytes / non_null);
}

bool basicStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    auto inner = removeLowCardinalityAndNullable(removeNullable(data_type));
    if (inner->isValueRepresentedByNumber())
        return true;
    if (isStringOrFixedString(inner))
        return true;
    /// Pure null-count tracking (e.g. `Nullable(Array(...))`) is also useful.
    return isNullableOrLowCardinalityNullable(data_type);
}

StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsBasic>(description, data_type);
}

}
