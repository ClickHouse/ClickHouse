#include <cstddef>
#include <Storages/NamedCollectionsHelpers.h>

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

    explicit NumpyDataType(Endianness endianess_) : endianess(endianess_) {}
    virtual ~NumpyDataType() = default;

    Endianness getEndianness() const { return endianess; }

    virtual NumpyDataTypeIndex getTypeIndex() const = 0;
    virtual size_t getSize() const = 0;

private:
    Endianness endianess;
};

class NumpyDataTypeInt : public NumpyDataType
{
public:
    NumpyDataTypeInt(Endianness endianess, size_t size_, bool is_signed_) : NumpyDataType(endianess), size(size_), is_signed(is_signed_) {}

    NumpyDataTypeIndex getTypeIndex() const override
    {
        switch (size)
        {
            case 1: return is_signed ? NumpyDataTypeIndex::Int8 : NumpyDataTypeIndex::UInt8;
            case 2: return is_signed ? NumpyDataTypeIndex::Int16 : NumpyDataTypeIndex::UInt16;
            case 4: return is_signed ? NumpyDataTypeIndex::Int32 : NumpyDataTypeIndex::UInt32;
            case 8: return is_signed ? NumpyDataTypeIndex::Int64 : NumpyDataTypeIndex::UInt64;
            default:
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Incorrect int type with size {}", size);
        }
    }
    size_t getSize() const override { return size; }
    bool isSigned() const { return is_signed; }

private:
    size_t size;
    bool is_signed;
};

class NumpyDataTypeFloat : public NumpyDataType
{
public:
    NumpyDataTypeFloat(Endianness endianess, size_t size_) : NumpyDataType(endianess), size(size_) {}

    NumpyDataTypeIndex getTypeIndex() const override
    {
        switch (size)
        {
            case 4: return NumpyDataTypeIndex::Float32;
            case 8: return NumpyDataTypeIndex::Float64;
            default:
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Incorrect float type with size {}", size);
        }
    }
    size_t getSize() const override { return size; }

private:
    size_t size;
};

class NumpyDataTypeString : public NumpyDataType
{
public:
    NumpyDataTypeString(Endianness endianess, size_t size_) : NumpyDataType(endianess), size(size_) {}

    NumpyDataTypeIndex getTypeIndex() const override { return NumpyDataTypeIndex::String; }
    size_t getSize() const override { return size; }
private:
    size_t size;
};

class NumpyDataTypeUnicode : public NumpyDataType
{
public:
    NumpyDataTypeUnicode(Endianness endianess, size_t size_) : NumpyDataType(endianess), size(size_) {}

    NumpyDataTypeIndex getTypeIndex() const override { return NumpyDataTypeIndex::Unicode; }
    size_t getSize() const override { return size; }
private:
    size_t size;
};
