#include <GPU/GPUTypeMapping.h>

#if USE_GPU

#include <GPU/GPUColumns.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Compression/CompressionInfo.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::GPU
{

std::optional<GPUElementType> elementTypeOf(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::UInt8: return GPUElementType::UInt8;
        case TypeIndex::UInt16: return GPUElementType::UInt16;
        case TypeIndex::UInt32: return GPUElementType::UInt32;
        case TypeIndex::UInt64: return GPUElementType::UInt64;
        case TypeIndex::Int8: return GPUElementType::Int8;
        case TypeIndex::Int16: return GPUElementType::Int16;
        case TypeIndex::Int32: return GPUElementType::Int32;
        case TypeIndex::Int64: return GPUElementType::Int64;
        case TypeIndex::Float32: return GPUElementType::Float32;
        case TypeIndex::Float64: return GPUElementType::Float64;
        default: return {};
    }
}

GPUElementType elementTypeOrThrow(const IDataType & type)
{
    if (const auto element_type = elementTypeOf(type))
        return *element_type;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Columns of {} are not supported on the GPU yet", type.getName());
}

std::optional<GPUElementType> filterElementTypeOf(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Date: return GPUElementType::UInt16;
        case TypeIndex::Date32: return GPUElementType::Int32;
        case TypeIndex::DateTime: return GPUElementType::UInt32;
        default: return elementTypeOf(type);
    }
}

std::optional<GPUCodec> codecOf(UInt8 method_byte)
{
    switch (method_byte)
    {
        case static_cast<UInt8>(CompressionMethodByte::LZ4): return GPUCodec::LZ4;
        case static_cast<UInt8>(CompressionMethodByte::ZSTD): return GPUCodec::ZSTD;
    }
    return {};
}

std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size)
{
    if (column.size() != num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Column {} holds {} rows, expected {}", column.getName(), column.size(), num_rows);

    const std::string_view raw = column.getRawData();
    if (raw.size() != num_rows * element_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of {} rows holds {} bytes of values, expected {}",
            column.getName(),
            num_rows,
            raw.size(),
            num_rows * element_size);

    return raw;
}

namespace
{

template <typename T>
char * resizeAndGetValueBytes(IColumn & column, size_t num_rows)
{
    auto * vector = typeid_cast<ColumnVector<T> *>(&column);
    if (!vector)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot copy {}-byte values out of the device into a column of {}",
            sizeof(T),
            column.getName());

    auto & data = vector->getData();
    if (!data.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot copy values into a column of {} that already holds {} rows",
            column.getName(),
            data.size());

    data.resize(num_rows);
    return reinterpret_cast<char *>(data.data());
}

char * resizeAndGetValueBytes(IColumn & column, size_t num_rows, GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::UInt8: return resizeAndGetValueBytes<UInt8>(column, num_rows);
        case GPUElementType::UInt16: return resizeAndGetValueBytes<UInt16>(column, num_rows);
        case GPUElementType::UInt32: return resizeAndGetValueBytes<UInt32>(column, num_rows);
        case GPUElementType::UInt64: return resizeAndGetValueBytes<UInt64>(column, num_rows);
        case GPUElementType::Int8: return resizeAndGetValueBytes<Int8>(column, num_rows);
        case GPUElementType::Int16: return resizeAndGetValueBytes<Int16>(column, num_rows);
        case GPUElementType::Int32: return resizeAndGetValueBytes<Int32>(column, num_rows);
        case GPUElementType::Int64: return resizeAndGetValueBytes<Int64>(column, num_rows);
        case GPUElementType::Float32: return resizeAndGetValueBytes<Float32>(column, num_rows);
        case GPUElementType::Float64: return resizeAndGetValueBytes<Float64>(column, num_rows);
        case GPUElementType::String: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Values of GPU element type {} for a column of fixed-width values", element_type);
}

}

HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type)
{
    return {element_type, resizeAndGetValueBytes(column, num_rows, element_type), num_rows};
}


GPUElementType sumResultTypeFor(GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::UInt8:
        case GPUElementType::UInt16:
        case GPUElementType::UInt32:
        case GPUElementType::UInt64:
            return GPUElementType::UInt64;
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return GPUElementType::Int64;
        case GPUElementType::Float32:
        case GPUElementType::Float64:
            return GPUElementType::Float64;
        case GPUElementType::String:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "A sum of GPU element type {}, which has none", element_type);
}

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name)
{
    if (aggregate_function_name == "sum")
        return GPUAggregationKind::Sum;
    if (aggregate_function_name == "min")
        return GPUAggregationKind::Min;
    if (aggregate_function_name == "max")
        return GPUAggregationKind::Max;
    if (aggregate_function_name == "count")
        return GPUAggregationKind::Count;
    if (aggregate_function_name == "avg")
        return GPUAggregationKind::Mean;

    return {};
}

String aggregationName(GPUAggregationKind aggregation)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Sum: return "sum";
        case GPUAggregationKind::Min: return "min";
        case GPUAggregationKind::Max: return "max";
        case GPUAggregationKind::Count: return "count";
        case GPUAggregationKind::Mean: return "avg";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU aggregation {}", aggregation);
}

std::optional<GPUElementType> keyElementTypeOf(const IDataType & type)
{
    if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(&type))
    {
        switch (fixed_string->getN())
        {
            case 1: return GPUElementType::UInt8;
            case 2: return GPUElementType::UInt16;
            case 4: return GPUElementType::UInt32;
            case 8: return GPUElementType::UInt64;
            default: return {};
        }
    }
    return elementTypeOf(type);
}

std::vector<GPUElementType> groupByKeysOrThrow(const DataTypes & key_types)
{
    std::vector<GPUElementType> keys;
    keys.reserve(key_types.size());
    for (const auto & key_type : key_types)
    {
        if (isString(*key_type))
        {
            keys.push_back(GPUElementType::String);
            continue;
        }
        const auto element_type = keyElementTypeOf(*key_type);
        if (!element_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A key of {} in a `GROUP BY` on the GPU", key_type->getName());
        keys.push_back(*element_type);
    }
    return keys;
}

namespace
{

bool canAggregateOnDevice(const IDataType * argument_type, const IDataType & result_type, GPUAggregationKind aggregation, bool grouped)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Count:
            return grouped && elementTypeOf(result_type) == GPUElementType::UInt64;
        case GPUAggregationKind::Mean:
        {
            /// cuDF sums for `avg` in the argument's width: 64-bit integers overflow, a `Float32` loses precision.
            const auto element_type = argument_type ? elementTypeOf(*argument_type) : std::nullopt;
            return grouped && element_type && *element_type != GPUElementType::Float32
                && (!isInteger(*element_type) || sizeOf(*element_type) <= 4)
                && elementTypeOf(result_type) == GPUElementType::Float64;
        }
        case GPUAggregationKind::Sum:
        {
            const auto element_type = argument_type ? elementTypeOf(*argument_type) : std::nullopt;
            /// cuDF groups a sum of a `Float32` into a `Float32`, where ClickHouse sums it into a `Float64`.
            return element_type && !(grouped && *element_type == GPUElementType::Float32)
                && elementTypeOf(result_type) == sumResultTypeFor(*element_type);
        }
        case GPUAggregationKind::Min:
        case GPUAggregationKind::Max:
        {
            /// Not of floats: cuDF compares a `NaN` as is, where ClickHouse skips it once it has a number.
            const auto column_type = argument_type ? columnTypeOf(*argument_type) : std::nullopt;
            return column_type && *column_type != GPUElementType::Float32 && *column_type != GPUElementType::Float64
                && columnTypeOf(result_type) == column_type;
        }
    }
    return false;
}

}

bool canAggregateOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations)
{
    if (argument_types.empty() || argument_types.size() != result_types.size() || argument_types.size() != aggregations.size())
        return false;

    for (const auto & key_type : key_types)
    {
        if (isString(*key_type))
            continue;
        const auto key_element_type = keyElementTypeOf(*key_type);
        if (!key_element_type || !isInteger(*key_element_type))
            return false;
    }

    const bool grouped = !key_types.empty();
    bool averages = false;
    bool extremes_of_strings = false;
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!canAggregateOnDevice(argument_types[i].get(), *result_types[i], aggregations[i], grouped))
            return false;
        averages |= aggregations[i] == GPUAggregationKind::Mean;
        extremes_of_strings |= (aggregations[i] == GPUAggregationKind::Min || aggregations[i] == GPUAggregationKind::Max)
            && columnTypeOf(*argument_types[i]) == GPUElementType::String;
    }

    /// `CudfGroupBy` combines `min` and `max` of strings from per-batch partial results, which `avg` cannot be.
    return !(averages && extremes_of_strings);
}
}

#endif
