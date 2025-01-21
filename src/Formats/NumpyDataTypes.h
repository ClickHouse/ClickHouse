#pragma once
#include <cstddef>
#include <Storages/NamedCollectionsHelpers.h>

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

enum class NumpyDataTypeIndex
{
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    String,
    Unicode,
};

class NumpyDataType
{
public:
    enum Endianness
    {
        LITTLE,
        BIG,
        NONE,
    };
    NumpyDataTypeIndex type_index;

    explicit NumpyDataType(Endianness endianness_) : endianness(endianness_) {}
    virtual ~NumpyDataType() = default;

    Endianness getEndianness() const { return endianness; }

    virtual NumpyDataTypeIndex getTypeIndex() const = 0;

private:
    Endianness endianness;
};

class NumpyDataTypeInt : public NumpyDataType
{
public:
    NumpyDataTypeInt(Endianness endianness, size_t size_, bool is_signed_) : NumpyDataType(endianness), size(size_), is_signed(is_signed_)
    {
        switch (size)
        {
            case 1: type_index = is_signed ? NumpyDataTypeIndex::Int8 : NumpyDataTypeIndex::UInt8; break;
            case 2: type_index = is_signed ? NumpyDataTypeIndex::Int16 : NumpyDataTypeIndex::UInt16; break;
            case 4: type_index = is_signed ? NumpyDataTypeIndex::Int32 : NumpyDataTypeIndex::UInt32; break;
            case 8: type_index = is_signed ? NumpyDataTypeIndex::Int64 : NumpyDataTypeIndex::UInt64; break;
            default:
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Incorrect int type with size {}", size);
        }
    }

    NumpyDataTypeIndex getTypeIndex() const override
    {
        return type_index;
    }
    bool isSigned() const { return is_signed; }

private:
    size_t size;
    bool is_signed;
};

class NumpyDataTypeFloat : public NumpyDataType
{
public:
    NumpyDataTypeFloat(Endianness endianness, size_t size_) : NumpyDataType(endianness), size(size_)
    {
        switch (size)
        {
            case 2: type_index = NumpyDataTypeIndex::Float16; break;
            case 4: type_index = NumpyDataTypeIndex::Float32; break;
            case 8: type_index = NumpyDataTypeIndex::Float64; break;
            default:
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Numpy float type with size {} is not supported", size);
        }
    }

    NumpyDataTypeIndex getTypeIndex() const override
    {
        return type_index;
    }
private:
    size_t size;
};

class NumpyDataTypeString : public NumpyDataType
{
public:
    NumpyDataTypeString(Endianness endianness, size_t size_) : NumpyDataType(endianness), size(size_)
    {
        type_index = NumpyDataTypeIndex::String;
    }

    NumpyDataTypeIndex getTypeIndex() const override { return type_index; }
    size_t getSize() const { return size; }
private:
    size_t size;
};

class NumpyDataTypeUnicode : public NumpyDataType
{
public:
    NumpyDataTypeUnicode(Endianness endianness, size_t size_) : NumpyDataType(endianness), size(size_)
    {
        type_index = NumpyDataTypeIndex::Unicode;
    }

    NumpyDataTypeIndex getTypeIndex() const override { return type_index; }
    size_t getSize() const { return size * 4; }
private:
    size_t size;
};
