#include <Compression/CompressionCodecDelta.h>
#include <common/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

void CompressionCodecDelta::setDataType(DataTypePtr _data_type)
{
    data_type = _data_type;
    element_size = data_type->getSizeOfValueInMemory();
    if (delta_type == 100)
    {
        if (!data_type->isNumber())
            delta_type = 0;
        else if (data_type->isUnsignedInteger())
            delta_type = 2;
        else if (data_type->isInteger())
            delta_type = 1;
        else
            delta_type = 3;
    }
};

size_t CompressionCodecDelta::writeHeader(char* header)
{
    *header = bytecode;
    unalignedStore(header + 1, delta_type);
    unalignedStore(header + 2, element_size);
    return 1 + sizeof(delta_type) + sizeof(element_size);
}

size_t CompressionCodecDelta::parseHeader(const char* header)
{
    delta_type = unalignedLoad<uint8_t>(header);
    element_size = unalignedLoad<uint16_t>(header + 1);

    return sizeof(delta_type) + sizeof(element_size);
}


void CompressionCodecDelta::compress_bytes(char* source, char* dest, size_t size) const
{
    char* tmp_arr = new char[element_size] {};
    for (size_t i = 0; i < size / element_size; ++i)
    {
        for (size_t j = 0; j < element_size; ++j)
            dest[i * element_size + j] = source[i * element_size + j] - tmp_arr[j];

        memcpy(tmp_arr, source + i * element_size, element_size);
    }
}

template <typename T>
void CompressionCodecDelta::compress_num(char* source, char* dest, size_t size) const
{
    T* arr = reinterpret_cast<T*>(source), *dest_arr = reinterpret_cast<T*>(dest);
    T elem = 0;
    for (size_t i = 0; i < size / element_size; ++i)
    {
        dest_arr[i] = elem - arr[i];
        memcpy(&elem, arr + i * element_size, element_size);
    }
}

size_t CompressionCodecDelta::compress(char* source, char* dest, int inputSize, int)
{
    if (inputSize % element_size)
        throw Exception("Data type does not fit input size.", ErrorCodes::LOGICAL_ERROR);

    switch (delta_type)
    {
        case 0:
            compress_bytes(source, dest, inputSize);
            break;
        case 1:
            switch (element_size)
            {
                case 1: compress_num<int8_t>(source, dest, inputSize); break;
                case 2: compress_num<int16_t>(source, dest, inputSize); break;
                case 4: compress_num<int32_t>(source, dest, inputSize); break;
                case 8: compress_num<int64_t>(source, dest, inputSize); break;
                default: compress_num<int8_t>(source, dest, inputSize); break;
            }
            break;
        case 2:
            switch (element_size)
            {
                case 1: compress_num<uint8_t>(source, dest, inputSize); break;
                case 2: compress_num<uint16_t>(source, dest, inputSize); break;
                case 4: compress_num<uint32_t>(source, dest, inputSize); break;
                case 8: compress_num<uint64_t>(source, dest, inputSize); break;
                default: compress_num<uint8_t>(source, dest, inputSize); break;
            }
            break;
        case 3:
            switch (element_size)
            {
                case 4: compress_num<float>(source, dest, inputSize); break;
                case 8: compress_num<double>(source, dest, inputSize); break;
                default: compress_num<float>(source, dest, inputSize); break;
            }
            break;
        default:
            memcpy(dest, source, inputSize);
            break;
    }
    return inputSize;
}

void CompressionCodecDelta::decompress_bytes(char* source, char* dest, size_t size) const
{
    char* tmp_arr = new char[element_size] {};
    for (size_t i = 0; i < size / element_size; ++i)
    {
        for (size_t j = 0; j < element_size; ++j)
            dest[i * element_size + j] = source[i * element_size + j] + tmp_arr[j];

        memcpy(tmp_arr, dest + i * element_size, element_size);
    }
}

template <typename T>
void CompressionCodecDelta::decompress_num(char* source, char* dest, size_t size) const
{
    T* arr = reinterpret_cast<T*>(source), *dest_arr = reinterpret_cast<T*>(dest);
    T elem = 0;
    for (size_t i = 0; i < size / element_size; ++i)
    {
        dest_arr[i] = elem + arr[i];
        memcpy(&elem, arr + i * element_size, element_size);
    }
}

size_t CompressionCodecDelta::decompress(char* source, char* dest, int inputSize, int)
{
    if (inputSize % element_size)
        throw Exception("Data type does not fit input size.", ErrorCodes::LOGICAL_ERROR);

    switch (delta_type)
    {
        case 0:
            decompress_bytes(source, dest, inputSize);
            break;
        case 1:
            switch (element_size)
            {
                case 1: decompress_num<int8_t>(source, dest, inputSize); break;
                case 2: decompress_num<int16_t>(source, dest, inputSize); break;
                case 4: decompress_num<int32_t>(source, dest, inputSize); break;
                case 8: decompress_num<int64_t>(source, dest, inputSize); break;
                default: decompress_num<int8_t>(source, dest, inputSize); break;
            }
            break;
        case 2:
            switch (element_size)
            {
                case 1: decompress_num<uint8_t>(source, dest, inputSize); break;
                case 2: decompress_num<uint16_t>(source, dest, inputSize); break;
                case 4: decompress_num<uint32_t>(source, dest, inputSize); break;
                case 8: decompress_num<uint64_t>(source, dest, inputSize); break;
                default: decompress_num<uint8_t>(source, dest, inputSize); break;
            }
            break;
        case 3:
            switch (element_size)
            {
                case 4: decompress_num<float>(source, dest, inputSize); break;
                case 8: decompress_num<double>(source, dest, inputSize); break;
                default: decompress_num<float>(source, dest, inputSize); break;
            }
            break;
        default:
            memcpy(dest, source, inputSize);
            break;
    }
    return inputSize;
}


static CodecPtr create(const ASTPtr &arguments) {
    if (!arguments)
        return std::make_shared<CompressionCodecDelta>();

    if (arguments->children.size() != 2)
        throw Exception("Delta codec can optionally have only two arguments",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral *arg_size = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg_size || arg_size->value.getType() != Field::Types::UInt64)
        throw Exception("Element size for Delta codec must be UInt16 literal",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const ASTLiteral *arg_type = typeid_cast<const ASTLiteral *>(arguments->children[1].get());
    if (!arg_type || arg_type->value.getType() != Field::Types::UInt64)
        throw Exception("Delta type (bytes, int, uint, float) parameter for Delta codec must be UInt8 literal",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<CompressionCodecDelta>(
            static_cast<uint16_t>(arg_size->value.get<uint16_t>()),
            static_cast<uint8_t>(arg_type->value.get<uint8_t>())
    );
}

CodecPtr createSimple()
{
    return std::make_shared<CompressionCodecDelta>();
}

void registerCodecDelta(CompressionCodecFactory & factory)
{
    factory.registerCodec("Delta", create);
    factory.registerCodecBytecode(CompressionCodecDelta::bytecode, createSimple);
}

}