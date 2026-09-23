#include <GPU/GPUTypeMapping.h>

#if USE_GPU

#include <Columns/ColumnVector.h>
#include <Compression/CompressionInfo.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

    throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} cannot be sent to a GPU", type.getName());
}

std::vector<GPUElementType> elementTypesOrThrow(const DataTypes & types)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(types.size());

    for (const auto & type : types)
        element_types.push_back(elementTypeOrThrow(*type));

    return element_types;
}

std::vector<size_t> elementSizesOf(const std::vector<GPUElementType> & element_types)
{
    std::vector<size_t> sizes;
    sizes.reserve(element_types.size());

    for (const GPUElementType element_type : element_types)
        sizes.push_back(sizeOf(element_type));

    return sizes;
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
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU element type {}", element_type);
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

std::optional<GPUAggregationKind> aggregationOf(const String & aggregate_function_name)
{
    if (aggregate_function_name == "sum")
        return GPUAggregationKind::Sum;
    if (aggregate_function_name == "min")
        return GPUAggregationKind::Min;
    if (aggregate_function_name == "max")
        return GPUAggregationKind::Max;

    return {};
}

String aggregationName(GPUAggregationKind aggregation)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Sum: return "sum";
        case GPUAggregationKind::Min: return "min";
        case GPUAggregationKind::Max: return "max";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU aggregation {}", aggregation);
}

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation)
{
    const auto element_type = elementTypeOf(argument_type);
    if (!element_type)
        return false;

    if (aggregation == GPUAggregationKind::Sum)
        return elementTypeOf(result_type) == sumResultTypeFor(*element_type);

    return elementTypeOf(result_type) == element_type;
}

GPUElementType reducibleElementTypeOrThrow(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation)
{
    if (canReduceOnDevice(argument_type, result_type, aggregation))
        return *elementTypeOf(argument_type);

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot reduce a column of {} into {} by `{}` on a GPU",
        argument_type.getName(),
        result_type.getName(),
        aggregationName(aggregation));
}

bool canGroupByReduceOnDevice(
    const DataTypes & key_types,
    const DataTypes & argument_types,
    const DataTypes & result_types,
    const std::vector<GPUAggregationKind> & aggregations)
{
    if (key_types.empty() || argument_types.empty() || argument_types.size() != result_types.size()
        || argument_types.size() != aggregations.size())
        return false;

    if (key_types.size() > max_group_by_keys || argument_types.size() > max_group_by_values)
        return false;

    /// The device packs the keys of a row into one integer of `max_group_by_key_bytes`, which a
    /// float could only join by its bytes, where ClickHouse groups by its value.
    size_t key_bytes = 0;
    for (const auto & key_type : key_types)
    {
        const auto key_element_type = elementTypeOf(*key_type);
        if (!key_element_type || !isInteger(*key_element_type))
            return false;
        key_bytes += sizeOf(*key_element_type);
    }
    if (key_bytes > max_group_by_key_bytes)
        return false;

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!canReduceOnDevice(*argument_types[i], *result_types[i], aggregations[i]))
            return false;
    }

    return true;
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
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GPU element type {}", element_type);
}

}

HostColumnView resizeForElementType(IColumn & column, size_t num_rows, GPUElementType element_type)
{
    return {element_type, resizeAndGetValueBytes(column, num_rows, element_type), num_rows};
}

}

#endif
