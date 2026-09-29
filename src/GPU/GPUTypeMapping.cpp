#include <GPU/GPUTypeMapping.h>

#if USE_GPU

#include <Columns/ColumnVector.h>
#include <Compression/CompressionInfo.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <algorithm>

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

}

#endif
