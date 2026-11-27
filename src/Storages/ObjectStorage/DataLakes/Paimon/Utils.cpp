#include <algorithm>
#include <cmath>
#include <ctime>
#include <filesystem>
#include <ranges>
#include <utility>
#include <vector>
#include <boost/algorithm/string/predicate.hpp>
#include <double-conversion/double-conversion.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Types.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <base/Decimal_fwd.h>
#include <base/arithmeticOverflow.h>
#include <base/types.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Poco/Logger.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/UTF8Helpers.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Types.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
extern const int PATH_ACCESS_DENIED;
}
}
namespace Paimon
{

const std::bitset<128> PathEscape::CHAR_TO_ESCAPE = []()
{
    std::bitset<128> bitset;

    for (unsigned char c = 0; c < ' '; ++c)
    {
        bitset.set(c);
    }

    const unsigned char clist[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
                                   0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, '"',
                                   '#',  '%',  '\'', '*',  '/',  ':',  '=',  '?',  '\\', 0x7F, '{',  '}',  '[',  ']',  '^'};

    for (unsigned char c : clist)
    {
        bitset.set(c);
    }

    return bitset;
}();

template <typename T>
static String formatQuoted(T x)
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

static String formatJavaFloatingPoint(double x, bool is_float)
{
    if (std::isnan(x))
        return "NaN";
    if (std::isinf(x))
        return x > 0 ? "Infinity" : "-Infinity";

    using double_conversion::DoubleToStringConverter;
    char digits[DoubleToStringConverter::kBase10MaximalLength + 1];
    bool sign = false;
    int length = 0;
    int point = 0;
    DoubleToStringConverter::DoubleToAscii(
        x,
        is_float ? DoubleToStringConverter::SHORTEST_SINGLE : DoubleToStringConverter::SHORTEST,
        0,
        digits,
        sizeof(digits),
        &sign,
        &length,
        &point);

    /// When one digit is enough, Java picks the closest of the decimals with one or two digits.
    if (length == 1)
    {
        DoubleToStringConverter::DoubleToAscii(x, DoubleToStringConverter::PRECISION, 2, digits, sizeof(digits), &sign, &length, &point);
        while (length > 1 && digits[length - 1] == '0')
            --length;
    }

    /// The value is 0.<digits> * 10^point.
    String res = sign ? "-" : "";
    if (-3 < point && point <= 7)
    {
        if (point <= 0)
        {
            res += "0.";
            res.append(-point, '0');
            res.append(digits, length);
        }
        else if (point >= length)
        {
            res.append(digits, length);
            res.append(point - length, '0');
            res += ".0";
        }
        else
        {
            res.append(digits, point);
            res += '.';
            res.append(digits + point, length - point);
        }
    }
    else
    {
        res += digits[0];
        res += '.';
        if (length > 1)
            res.append(digits + 1, length - 1);
        else
            res += '0';
        res += 'E';
        res += std::to_string(point - 1);
    }
    return res;
}

String formatJavaFloat(Float32 x)
{
    return formatJavaFloatingPoint(static_cast<double>(x), true);
}

String formatJavaDouble(Float64 x)
{
    return formatJavaFloatingPoint(x, false);
}

static bool isJavaWhitespace(UInt32 code_point)
{
    return (code_point >= 0x09 && code_point <= 0x0D) || (code_point >= 0x1C && code_point <= 0x20) || code_point == 0x1680
        || (code_point >= 0x2000 && code_point <= 0x2006) || (code_point >= 0x2008 && code_point <= 0x200A) || code_point == 0x2028
        || code_point == 0x2029 || code_point == 0x205F || code_point == 0x3000;
}

bool isNullOrWhitespaceOnly(std::string_view value)
{
    const char * pos = value.data();
    const char * end = pos + value.size();
    while (pos < end)
    {
        const size_t length = UTF8::seqLength(static_cast<UInt8>(*pos));
        if (length > static_cast<size_t>(end - pos))
            return false;
        const auto code_point = UTF8::convertUTF8ToCodePoint(pos, length);
        if (!code_point || !isJavaWhitespace(*code_point))
            return false;
        pos += length;
    }
    return true;
}

template <typename T>
String formatDecimal(const Decimal<T> & x, UInt32 scale)
{
    WriteBufferFromOwnString wb;
    writeText(x, scale, wb, true);
    return wb.str();
}

static std::pair<Int64, Int64> splitIntoWholeSecondsAndNanoOfSecond(const Paimon::Timestamp & timestamp)
{
    Int64 whole_seconds = timestamp.millisecond / 1000;
    Int64 milli_of_second = timestamp.millisecond % 1000;
    /// Division truncates towards zero, while `nano_of_millisecond` counts forward from
    /// `millisecond`, so a negative remainder belongs to the preceding second.
    if (milli_of_second < 0)
    {
        --whole_seconds;
        milli_of_second += 1000;
    }
    return {whole_seconds, milli_of_second * 1'000'000 + timestamp.nano_of_millisecond};
}

/// Reproduces Java's `LocalDateTime.toString`, which is how Paimon names a partition directory:
/// `InternalRowPartitionComputer.generatePartValues` calls `Timestamp.toString` - the option
/// `partition.legacy-name` that selects this defaults to true - and that is
/// `toLocalDateTime().toString()`. `HH:mm` is always printed, `:ss` only when the second or the
/// fraction is non-zero, and the fraction with 3, 6 or 9 digits, whichever is the shortest that is
/// not lossy.
///
/// Paimon applies no time zone here: `Timestamp.toLocalDateTime` splits the raw epoch millisecond
/// arithmetically, so the name is rendered as if in UTC. That holds for
/// `TIMESTAMP WITH LOCAL TIME ZONE` as well, which is why the time zone of the column must not be
/// used - it is the server time zone for that type, and would name a directory Paimon never wrote.
static String formatTimestampAsPartitionName(const Paimon::Timestamp & timestamp)
{
    const auto [whole_seconds, nano_of_second] = splitIntoWholeSecondsAndNanoOfSecond(timestamp);

    WriteBufferFromOwnString wb;
    static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");
    writeDateTimeText<'-', ':', 'T'>(static_cast<time_t>(whole_seconds), wb, utc_time_zone);
    String res = wb.str();

    if (nano_of_second == 0)
    {
        /// The text is `YYYY-MM-DDTHH:MM:SS`, so this tests the second alone. Java omits `:ss` only
        /// when the second and the fraction are both zero.
        if (res.ends_with(":00"))
            res.resize(res.length() - 3);
        return res;
    }

    if (nano_of_second % 1'000'000 == 0)
        return res + fmt::format(".{:03}", nano_of_second / 1'000'000);
    if (nano_of_second % 1'000 == 0)
        return res + fmt::format(".{:06}", nano_of_second / 1'000);
    return res + fmt::format(".{:09}", nano_of_second);
}

String formatDateCastToString(Int32 epoch_day)
{
    WriteBufferFromOwnString wb;
    static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");
    writeDateText(ExtendedDayNum(epoch_day), wb, utc_time_zone);
    return wb.str();
}

String formatTimeCastToString(Int32 millis_of_day, UInt32 precision)
{
    Int32 time = millis_of_day;
    while (time < 0)
        time += 86'400'000;

    auto append_two_digits = [](String & out, Int32 value)
    {
        out += static_cast<char>('0' + (value / 10) % 10);
        out += static_cast<char>('0' + value % 10);
    };

    String res;
    append_two_digits(res, time / 3'600'000);
    res += ':';
    append_two_digits(res, time % 3'600'000 / 60'000);
    res += ':';
    append_two_digits(res, time % 60'000 / 1000);

    if (precision > 0)
    {
        res += '.';
        Int32 millis = time % 1000;
        /// At least one fractional digit, then up to `precision` digits while the remainder is non-zero.
        while (precision > 0)
        {
            res += static_cast<char>('0' + millis / 100);
            millis = millis % 100 * 10;
            if (millis == 0)
                break;
            --precision;
        }
    }
    return res;
}

String formatTimestampCastToString(const Paimon::Timestamp & timestamp, UInt32 precision, const DateLUTImpl & time_zone)
{
    const auto [whole_seconds, nano_of_second] = splitIntoWholeSecondsAndNanoOfSecond(timestamp);

    WriteBufferFromOwnString wb;
    writeDateTimeText<'-', ':', ' '>(static_cast<time_t>(whole_seconds), wb, time_zone);
    String res = wb.str();

    String fraction = fmt::format("{:09}", nano_of_second);
    while (fraction.size() > precision && fraction.back() == '0')
        fraction.pop_back();
    if (!fraction.empty())
        res += "." + fraction;
    return res;
}

static bool isLegacyPartitionName(const PaimonTableSchema & table_schema)
{
    auto it = table_schema.options.find(PAIMON_PARTITION_LEGACY_NAME);
    if (it == table_schema.options.end() || boost::iequals(it->second, "true"))
        return true;
    if (boost::iequals(it->second, "false"))
        return false;
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid value '{}' of the Paimon table option `{}`, expected `true` or `false`",
        it->second,
        PAIMON_PARTITION_LEGACY_NAME);
}

/// The value of a timestamp partition key, at a scale that represents it without loss.
static DecimalField<DateTime64> toDateTime64Field(const Paimon::Timestamp & timestamp, UInt32 precision)
{
    chassert(precision <= 9);

    /// The compact encoding holds a number of milliseconds whatever the declared precision is, so
    /// the value is reported with scale 3 rather than with the precision of the column: scaling down
    /// to a precision below 3 here would lose information, and reporting a scale that differs from
    /// the one of the column is harmless because `KeyCondition` converts the field to the type of
    /// the column and that conversion accounts for both scales.
    if (precision <= 3)
        return DecimalField<DateTime64>(DateTime64(timestamp.millisecond), 3);

    static constexpr Int64 POWERS_OF_TEN[] = {1, 10, 100, 1'000, 10'000, 100'000, 1'000'000};
    const Int64 millisecond_multiplier = POWERS_OF_TEN[precision - 3];
    const Int64 nanosecond_divisor = POWERS_OF_TEN[9 - precision];

    /// `nano_of_millisecond` counts forward from `millisecond` also when the latter is negative, so
    /// the two parts add up over the whole range.
    Int64 value = 0;
    if (common::mulOverflow(timestamp.millisecond, millisecond_multiplier, value)
        || common::addOverflow(value, static_cast<Int64>(timestamp.nano_of_millisecond / nanosecond_divisor), value))
        throw Exception(
            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
            "Paimon timestamp of {} ms and {} ns does not fit into DateTime64({})",
            timestamp.millisecond,
            timestamp.nano_of_millisecond,
            precision);

    return DecimalField<DateTime64>(DateTime64(value), precision);
}

DB::Row getPartitionFields(const String & partition_string, const PaimonTableSchema & table_schema)
{
    Paimon::BinaryRow partition(partition_string);
    auto get_partition_value = [&partition](Int32 i, Paimon::DataType & data_type) -> Field
    {
        if (data_type.clickhouse_data_type->isNullable() && partition.isNullAt(i))
        {
            return Null();
        }

        switch (data_type.root_type)
        {
            case RootDataType::CHAR:
            case RootDataType::VARCHAR:
                return Field(partition.getString(i));
            case RootDataType::BOOLEAN: {
                return Field(partition.getBoolean(i));
            }
            case RootDataType::DECIMAL: {
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal32 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return DecimalField<Decimal32>(
                        partition.getDecimal<Int32>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal64 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return DecimalField<Decimal64>(
                        partition.getDecimal<Int64>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal128 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return DecimalField<Decimal128>(
                        partition.getDecimal<Int128>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal256 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return DecimalField<Decimal256>(
                        partition.getDecimal<Int256>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type {}", data_type.clickhouse_data_type->getName());
            }
            case RootDataType::TINYINT:
                return Field(partition.getByte(i));
            case RootDataType::SMALLINT:
                return Field(partition.getShort(i));
            case RootDataType::INTEGER:
            case RootDataType::DATE:
            case RootDataType::TIME_WITHOUT_TIME_ZONE:
                return Field(partition.getInt(i));
            case RootDataType::BIGINT:
                return Field(partition.getLong(i));
            case RootDataType::FLOAT:
                return Field(partition.getFloat(i));
            case RootDataType::DOUBLE:
                return Field(partition.getDouble(i));
            case RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE:
            case RootDataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                const auto * type = typeid_cast<const DataTypeDateTime64 *>(removeNullable(data_type.clickhouse_data_type).get());
                LOG_TEST(
                    &Poco::Logger::get("getPartitionString"), "getPrecision: {}, getScale: {}", type->getPrecision(), type->getScale());
                return toDateTime64Field(
                    partition.getTimestamp(i, static_cast<Int32>(type->getScale())), type->getScale());
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type {} in partition", data_type.root_type);
        }
    };

    DB::Row partition_key_value;
    for (size_t i = 0; i < table_schema.partition_keys.size(); ++i)
    {
        auto it = table_schema.fields_by_name_indexes.find(table_schema.partition_keys[i]);
        if (it == table_schema.fields_by_name_indexes.end() || it->second >= table_schema.fields.size())
        {
            throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} is not found in table schema fields: [{}]",
                table_schema.partition_keys[i],
                fmt::join(table_schema.fields_by_name_indexes, ","));
        }
        auto field = table_schema.fields[it->second];
        partition_key_value.emplace_back(get_partition_value(static_cast<Int32>(i), field.type));
    }
    return partition_key_value;
};

static String getPartitionString(Paimon::BinaryRow & partition, const PaimonTableSchema & table_schema, const String & partition_default_name)
{
    if (table_schema.partition_keys.empty())
        return "";

    const bool legacy_partition_name = isLegacyPartitionName(table_schema);
    auto get_partition_value = [&partition, &partition_default_name, legacy_partition_name](Int32 i, Paimon::DataType & data_type) -> String
    {
        if (data_type.clickhouse_data_type->isNullable() && partition.isNullAt(i))
        {
            /// TODO: support customer default partition name
            return partition_default_name;
        }

        switch (data_type.root_type)
        {
            case RootDataType::CHAR:
            case RootDataType::VARCHAR:
                return partition.getString(i);
            case RootDataType::BOOLEAN: {
                return partition.getBoolean(i) ? "true" : "false";
            }
            case RootDataType::DECIMAL: {
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal32 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return formatDecimal(
                        partition.getDecimal<Int32>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal64 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return formatDecimal(
                        partition.getDecimal<Int64>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal128 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return formatDecimal(
                        partition.getDecimal<Int128>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                if (const auto * decimal_type
                    = typeid_cast<const DataTypeDecimal256 *>(removeNullable(data_type.clickhouse_data_type).get()))
                    return formatDecimal(
                        partition.getDecimal<Int256>(i, decimal_type->getPrecision(), decimal_type->getScale()), decimal_type->getScale());
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type {}", data_type.clickhouse_data_type->getName());
            }
            case RootDataType::TINYINT:
                return formatQuoted(partition.getByte(i));
            case RootDataType::SMALLINT:
                return formatQuoted(partition.getShort(i));
            case RootDataType::INTEGER:
                return formatQuoted(partition.getInt(i));
            case RootDataType::DATE:
                if (legacy_partition_name)
                    return formatQuoted(partition.getInt(i));
                return formatDateCastToString(partition.getInt(i));
            case RootDataType::TIME_WITHOUT_TIME_ZONE:
                if (legacy_partition_name)
                    return formatQuoted(partition.getInt(i));
                return formatTimeCastToString(partition.getInt(i), data_type.time_precision);
            case RootDataType::BIGINT:
                return formatQuoted(partition.getLong(i));
            case RootDataType::FLOAT:
                return formatJavaFloat(partition.getFloat(i));
            case RootDataType::DOUBLE:
                return formatJavaDouble(partition.getDouble(i));
            case RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE:
            case RootDataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                const auto * type = typeid_cast<const DataTypeDateTime64 *>(removeNullable(data_type.clickhouse_data_type).get());
                LOG_TEST(
                    &Poco::Logger::get("getPartitionString"), "getPrecision: {}, getScale: {}", type->getPrecision(), type->getScale());
                const auto timestamp = partition.getTimestamp(i, static_cast<Int32>(type->getScale()));
                if (legacy_partition_name)
                    return formatTimestampAsPartitionName(timestamp);
                /// Paimon uses the writer JVM's zone; not the session zone, as parsed manifests are cached across queries.
                static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");
                const DateLUTImpl & time_zone = data_type.root_type == RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE
                    ? utc_time_zone
                    : DateLUT::serverTimezoneInstance();
                return formatTimestampCastToString(timestamp, type->getScale(), time_zone);
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type {} in partition", data_type.root_type);
        }
    };
    std::vector<std::pair<String, String>> partition_entries;
    LOG_TEST(&Poco::Logger::get("getPartitionString"), "table_schema.partition_keys size: {}", table_schema.partition_keys.size());
    for (size_t i = 0; i < table_schema.partition_keys.size(); ++i)
    {
        std::vector<std::pair<String, String>> partitions;
        auto it = table_schema.fields_by_name_indexes.find(table_schema.partition_keys[i]);
        if (it == table_schema.fields_by_name_indexes.end() || it->second >= table_schema.fields.size())
        {
            throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} is not found in table schema fields: [{}]",
                table_schema.partition_keys[i],
                fmt::join(table_schema.fields_by_name_indexes, ","));
        }
        auto field = table_schema.fields[it->second];
        auto field_value = get_partition_value(static_cast<Int32>(i), field.type);
        if (isNullOrWhitespaceOnly(field_value))
            field_value = partition_default_name;
        LOG_TEST(&Poco::Logger::get("getPartitionString"), "key: {} value: {} type: {}", field.name, field_value, field.type.root_type);
        partition_entries.emplace_back(field.name, field_value);
    }

    WriteBufferFromOwnString path_writer;
    size_t i = 0;
    for (const auto & entry : partition_entries)
    {
        if (i > 0)
        {
            writeString("/", path_writer);
        }
        writeString(Paimon::PathEscape::escapePathName(entry.first), path_writer);
        writeString("=", path_writer);
        writeString(Paimon::PathEscape::escapePathName(entry.second), path_writer);
        ++i;
    }
    if (!partition_entries.empty())
        writeString("/", path_writer);
    return path_writer.str();
};

String getBucketPath(const String & partition, Int32 bucket, const PaimonTableSchema & table_schema, const String & partition_default_name)
{
    Paimon::BinaryRow binary_row(partition);
    String partition_string = getPartitionString(binary_row, table_schema, partition_default_name);
    String bucket_path = fmt::format("bucket-{}", bucket);
    LOG_TEST(&Poco::Logger::get("getBucketPath"), "partition_string {}, bucket_path: {}", partition_string, bucket_path);
    if (!partition_string.empty())
    {
        bucket_path = partition_string + bucket_path;
    }
    return bucket_path;
}

void checkPathIsRelativeToTable(const String & path, std::string_view kind)
{
    const std::filesystem::path fs_path(path);
    if (fs_path.has_root_path() || std::ranges::any_of(fs_path, [](const auto & component) { return component == ".."; }))
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Paimon {} path `{}` must be relative to the table directory and must not contain `..`",
            kind,
            path);
}

String concatPath(std::initializer_list<String> paths)
{
    if (paths.size() == 1)
        return *paths.begin();
    auto join_paths = paths | std::views::filter([](String x) { return !x.empty(); });
    return fmt::to_string(fmt::join(join_paths, "."));
}

}
