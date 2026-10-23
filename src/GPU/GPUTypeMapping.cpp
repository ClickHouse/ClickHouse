#include <GPU/GPUTypeMapping.h>

#if USE_GPU

#include <GPU/GPUColumns.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Compression/CompressionCodecMultiple.h>
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

std::optional<GPUCodec> codecOf(UInt8 method_byte)
{
    switch (method_byte)
    {
        case static_cast<UInt8>(CompressionMethodByte::LZ4): return GPUCodec::LZ4;
        case static_cast<UInt8>(CompressionMethodByte::ZSTD): return GPUCodec::ZSTD;
        case static_cast<UInt8>(CompressionMethodByte::GDeflate): return GPUCodec::GDEFLATE;
    }
    return {};
}

std::optional<GPUCodec> deviceCodecOf(const ICompressionCodec & codec)
{
    if (const auto own = codecOf(codec.getMethodByte()))
        return own;

    const auto * multiple = typeid_cast<const CompressionCodecMultiple *>(&codec);
    if (!multiple)
        return {};

    const auto chain = multiple->getCodecs();
    if (chain.size() != 2 || chain[0]->getMethodByte() != static_cast<UInt8>(CompressionMethodByte::BitPacking))
        return {};
    return codecOf(chain[1]->getMethodByte());
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

std::vector<GPUElementType> filterElementTypesOrThrow(const DataTypes & types)
{
    std::vector<GPUElementType> element_types;
    element_types.reserve(types.size());
    for (const auto & type : types)
    {
        const auto element_type = filterElementTypeOf(*type);
        if (!element_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} in a `WHERE` on the GPU", type->getName());
        element_types.push_back(*element_type);
    }
    return element_types;
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

bool canReduceOnDevice(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation)
{
    /// Only a `GROUP BY` counts and averages, see `canGroupByOnDevice`.
    if (aggregation == GPUAggregationKind::Count || aggregation == GPUAggregationKind::Mean)
        return false;

    if (aggregation == GPUAggregationKind::Sum)
    {
        const auto element_type = elementTypeOf(argument_type);
        return element_type && elementTypeOf(result_type) == sumResultTypeFor(*element_type);
    }

    /// A `min` or a `max` is of the argument's type, and of values of varying width too, compared by their bytes.
    const auto column_type = columnTypeOf(argument_type);
    return column_type && columnTypeOf(result_type) == column_type;
}

bool canGroupByOnDevice(const IDataType * argument_type, const IDataType & result_type, GPUAggregationKind aggregation)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Count:
            return elementTypeOf(result_type) == GPUElementType::UInt64;
        case GPUAggregationKind::Mean:
        {
            /// cuDF averages integers through a sum in an `Int64`, which a group of 64-bit integers overflows where
            /// ClickHouse's `avg` does not; one of narrower integers does not, below 2^31 rows.
            const auto element_type = argument_type ? elementTypeOf(*argument_type) : std::nullopt;
            return element_type && (!isInteger(*element_type) || sizeOf(*element_type) <= 4)
                && elementTypeOf(result_type) == GPUElementType::Float64;
        }
        case GPUAggregationKind::Sum:
            /// cuDF sums a `Float32` into a `Float32`, where ClickHouse sums it into a `Float64`.
            if (argument_type && elementTypeOf(*argument_type) == GPUElementType::Float32)
                return false;
            [[fallthrough]];
        case GPUAggregationKind::Min:
        case GPUAggregationKind::Max:
            return argument_type && canReduceOnDevice(*argument_type, result_type, aggregation);
    }
    return false;
}

GPUElementType groupByResultTypeOrThrow(const IDataType * argument_type, const IDataType & result_type, GPUAggregationKind aggregation)
{
    if (!canGroupByOnDevice(argument_type, result_type, aggregation))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot group a column of {} into {} by `{}` on a GPU",
            argument_type ? argument_type->getName() : "nothing",
            result_type.getName(),
            aggregationName(aggregation));
    return *columnTypeOf(result_type);
}

GPUElementType reducibleElementTypeOrThrow(const IDataType & argument_type, const IDataType & result_type, GPUAggregationKind aggregation)
{
    if (canReduceOnDevice(argument_type, result_type, aggregation))
        return *columnTypeOf(argument_type);

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot reduce a column of {} into {} by `{}` on a GPU",
        argument_type.getName(),
        result_type.getName(),
        aggregationName(aggregation));
}

bool isStringKey(const IDataType & type)
{
    return isString(type);
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
    /// A date is grouped, joined and sent as the integer it is held as.
    switch (type.getTypeId())
    {
        case TypeIndex::Date: return GPUElementType::UInt16;
        case TypeIndex::Date32: return GPUElementType::Int32;
        case TypeIndex::DateTime: return GPUElementType::UInt32;
        default: return elementTypeOf(type);
    }
}

std::optional<GPUElementType> collectElementTypeOf(const IDataType & type)
{
    if (isStringKey(type))
        return GPUElementType::String;
    if (typeid_cast<const DataTypeFixedString *>(&type))
        return keyElementTypeOf(type);
    return filterElementTypeOf(type);
}

bool isFloatKey(const IDataType & type)
{
    return type.getTypeId() == TypeIndex::Float32 || type.getTypeId() == TypeIndex::Float64;
}

std::optional<GPUElementType> groupKeyElementTypeOf(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Float32: return GPUElementType::UInt32;
        case TypeIndex::Float64: return GPUElementType::UInt64;
        default: return keyElementTypeOf(type);
    }
}

std::vector<GPUElementType> groupByKeysOrThrow(const DataTypes & key_types)
{
    std::vector<GPUElementType> keys;
    keys.reserve(key_types.size());
    for (const auto & key_type : key_types)
    {
        if (isStringKey(*key_type))
        {
            keys.push_back(GPUElementType::String);
            continue;
        }
        const auto element_type = groupKeyElementTypeOf(*key_type);
        if (!element_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A key of {} in a `GROUP BY` on the GPU", key_type->getName());
        keys.push_back(*element_type);
    }
    return keys;
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

    /// `CudfGroupBy` takes keys of integers and strings, and floats as the integers of their bits.
    for (const auto & key_type : key_types)
    {
        if (isStringKey(*key_type))
            continue;
        const auto key_element_type = groupKeyElementTypeOf(*key_type);
        if (!key_element_type || !isInteger(*key_element_type))
            return false;
    }

    /// A `count` has no argument.
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!canGroupByOnDevice(argument_types[i].get(), *result_types[i], aggregations[i]))
            return false;
    }

    return true;
}
}

#endif
