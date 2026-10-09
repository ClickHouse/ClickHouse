#include <Storages/Statistics/StatisticsBasic.h>

#include <Columns/ColumnFixedString.h>
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
    DefaultCount = 1u << 2,
};

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
    auto full = column
        ->convertToFullColumnIfConst()
        ->convertToFullColumnIfSparse()
        ->convertToFullColumnIfLowCardinality();
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
        return s->byteSize();
    }
    return 0;
}

}


StatisticsBasic::StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeLowCardinalityAndNullable(removeNullable(data_type_)))
{
    tracks_numeric = canStatisticsTrackMinMax(data_type);
    tracks_string = isStringOrFixedString(data_type);

    /// Compute the column-level default once so `estimateEqual` can compare against the same
    /// value that `build` counts via `IColumn::isDefaultAt`.
    auto default_col = data_type->createColumn();
    default_col->insertDefault();
    column_default_field = (*default_col)[0];

    is_nullable = isNullableOrLowCardinalityNullable(data_type_) || column_default_field.isNull();
}

void StatisticsBasic::build(const ColumnPtr & column)
{
    const size_t column_size = column->size();

    const UInt64 defaults_in_block = column->getNumberOfDefaultRows();
    default_count += defaults_in_block;
    has_default_count = true;

    /// NULL rows in this block; used only to detect all-NULL blocks for the min/max guard below.
    const UInt64 nulls_in_block = is_nullable ? defaults_in_block : 0;

    if (tracks_numeric && nulls_in_block < column_size)
    {
        Field min_field;
        Field max_field;
        column->getExtremes(min_field, max_field, 0, column_size);

        StatisticsUtils::updateMin(min, min_field);
        StatisticsUtils::updateMax(max, max_field);
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
        StatisticsUtils::updateMin(min, other->min);
        StatisticsUtils::updateMax(max, other->max);
    }
    if (tracks_string)
        string_total_bytes += other->string_total_bytes;

    if (has_default_count)
    {
        default_count += other->default_count;
    }

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

    mask |= BasicFeatureMask::DefaultCount;
    writeIntBinary(mask, buf);

    if (tracks_numeric)
    {
        writeFieldBinary(min, buf);
        writeFieldBinary(max, buf);
    }
    if (tracks_string)
        writeIntBinary(string_total_bytes, buf);

    writeIntBinary(default_count, buf);
}

void StatisticsBasic::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    readIntBinary(row_count, buf);

    UInt8 mask = 0;
    readIntBinary(mask, buf);

    if (mask & BasicFeatureMask::NumericMinMax)
    {
        min = readFieldBinary(buf);
        max = readFieldBinary(buf);
    }

    if (mask & BasicFeatureMask::StringLengthSum)
    {
        readIntBinary(string_total_bytes, buf);
    }

    has_default_count = (mask & BasicFeatureMask::DefaultCount) != 0;
    if (has_default_count)
        readIntBinary(default_count, buf);
}

std::optional<Float64> StatisticsBasic::estimateLess(const Field & val) const
{
    if (!tracks_numeric)
        return std::nullopt;
    if (row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    /// Total non-NULL rows known to the part: the linear-interpolation domain. Only subtract the
    /// default count when it counts NULLs (Nullable column); for a non-Nullable column the defaults
    /// are ordinary values that belong to the interpolation domain.
    const UInt64 non_null = (is_nullable && has_default_count && default_count <= row_count)
        ? (row_count - default_count)
        : row_count;
    if (non_null == 0)
        return 0.0;

    return StatisticsUtils::interpolateLessLinear(val, min, max, non_null, data_type);
}

std::optional<Float64> StatisticsBasic::estimateEqual(const Field & val) const
{
    /// Only a non-Nullable column exposes an exact equality-to-default count. For a Nullable column
    /// the default is NULL, whose selectivity is served by `IS NULL`, not by `col = <literal>`.
    if (is_nullable || !has_default_count)
        return std::nullopt;

    /// Coerce the literal to the column type (e.g. parse '0'); a value that does not convert cannot
    /// be the default. The try-variant returns NULL instead of throwing on an unconvertible field.
    Field converted = tryConvertFieldToType(val, *data_type);
    if (converted.isNull())
        return std::nullopt;

    /// `default_count` was built via `IColumn::isDefaultAt` (column-level zero). Compare the
    /// converted value against the same column-level default rather than `IDataType::getDefault()`,
    /// which can differ: `FixedString(N)` has column default N zero bytes vs. type default "",
    /// and `Enum` has column default raw 0 vs. type default the first enumerator name.
    if (converted != column_default_field)
        return std::nullopt;

    return static_cast<Float64>(default_count);
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
    if (has_default_count)
    {
        sep("default_count");
        result += std::to_string(default_count);
    }
    if (first)
        result += "(empty)";
    return result;
}

Int64 StatisticsBasic::getStringLengthAvg() const
{
    /// Denominator = number of non-NULL rows we summed bytes over. When the column is Nullable the
    /// default count is the NULL count, so subtract it; otherwise every processed row contributed.
    const UInt64 non_null = (is_nullable && has_default_count && default_count <= row_count)
        ? (row_count - default_count)
        : row_count;
    if (non_null == 0)
        return 0;
    return static_cast<Int64>(string_total_bytes / non_null);
}

bool StatisticsBasic::isCompatibleWith(const IStatistics & other) const
{
    const auto * other_basic = typeid_cast<const StatisticsBasic *>(&other);
    if (!other_basic)
        return false;
    return tracks_numeric == other_basic->tracks_numeric
        && tracks_string == other_basic->tracks_string
        && has_default_count == other_basic->has_default_count;
}

bool basicStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & /*data_type*/)
{
    /// `basic` supports any column type: a default-value count (`getNumberOfDefaultRows`) is defined
    /// for every column, and the numeric min/max and string-length sub-statistics are populated only
    /// for the applicable types. Numeric types additionally get min/max; String/FixedString get the
    /// average length; Nullable columns get a NULL count (the default of a Nullable type is NULL).
    return true;
}

StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsBasic>(description, data_type);
}

}
