#include <ctime>
#include <ranges>
#include <utility>
#include <vector>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Types.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <base/Decimal_fwd.h>
#include <base/types.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
extern const int BAD_ARGUMENTS;
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

static String formatFloat(const Float64 x)
{
    DoubleConverter<true>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

    if (!result)
        throw Exception(ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print float or double number");

    return {buffer, buffer + builder.position()};
}

static String formatFloat(const Float32 x)
{
    DoubleConverter<true>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<true>::instance().ToShortestSingle(x, &builder);

    if (!result)
        throw Exception(ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print float or double number");

    return {buffer, buffer + builder.position()};
}

template <typename T>
String formatDecimal(const Decimal<T> & x, UInt32 scale)
{
    WriteBufferFromOwnString wb;
    writeText(x, scale, wb, true);
    return wb.str();
}

String formatDateTime(const DateTime64 & x, UInt32 scale, const DateLUTImpl & time_zone)
{
    WriteBufferFromOwnString wb;
    writeDateTimeText<'-', ':', 'T', '.', true>(x, scale, wb, time_zone);
    auto res = wb.str();
    if (res.ends_with(":00"))
        res = res.substr(0, res.length() - 3);
    return res;
}

String getPartitionString(Paimon::BinaryRow & partition, const PaimonTableSchema & table_schema, const String & partition_default_name)
{
    auto get_partition_value = [&partition, &partition_default_name](Int32 i, Paimon::DataType & data_type) -> String
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
            case RootDataType::DATE:
            case RootDataType::TIME_WITHOUT_TIME_ZONE:
                return formatQuoted(partition.getInt(i));
            case RootDataType::BIGINT:
                return formatQuoted(partition.getLong(i));
            case RootDataType::FLOAT:
                return formatFloat(partition.getFloat(i));
            case RootDataType::DOUBLE:
                return formatFloat(partition.getDouble(i));
            case RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE:
            case RootDataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                const auto * type = typeid_cast<const DataTypeDateTime64 *>(removeNullable(data_type.clickhouse_data_type).get());
                LOG_TEST(
                    &Poco::Logger::get("getPartitionString"), "getPrecision: {}, getScale: {}", type->getPrecision(), type->getScale());
                return formatDateTime(partition.getTimestamp(i, type->getScale()), 3, type->getTimeZone());
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
    writeString("/", path_writer);
    return path_writer.str();
};

String getBucketPath(String partition, Int32 bucket, const PaimonTableSchema & table_schema, const String & partition_default_name)
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

String concatPath(std::initializer_list<String> paths)
{
    if (paths.size() == 1)
        return *paths.begin();
    auto join_paths = paths | std::views::filter([](String x) { return !x.empty(); });
    return fmt::to_string(fmt::join(join_paths, "."));
}

}
