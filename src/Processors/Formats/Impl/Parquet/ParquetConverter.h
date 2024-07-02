#pragma once
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <base/Decimal_fwd.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>
#include <Common/PODArray.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename PhysicalType>
struct ToParquet
{
    using T = typename PhysicalType::c_type;
    T as(const DB::Field & value, const parquet::ColumnDescriptor &)
    {
        if constexpr (std::is_same_v<PhysicalType, parquet::Int32Type>)
            return static_cast<T>(value.get<Int64>());
        // parquet::BooleanType, parquet::Int64Type, parquet::FloatType, parquet::DoubleType
        return static_cast<T>(value.get<T>()); // FLOAT, DOUBLE, INT64
    }
};

template <>
struct ToParquet<parquet::ByteArrayType>
{
    using T = parquet::ByteArray;
    T as(const DB::Field & value, const parquet::ColumnDescriptor &)
    {
        assert(value.getType() == DB::Field::Types::String);
        const std::string & s = value.get<std::string>();
        const auto * const ptr = reinterpret_cast<const uint8_t *>(s.data());
        return parquet::ByteArray(static_cast<uint32_t>(s.size()), ptr);
    }
};

template <>
struct ToParquet<parquet::FLBAType>
{
    uint8_t buf[256];
    using T = parquet::FixedLenByteArray;
    T as(const DB::Field & value, const parquet::ColumnDescriptor & descriptor)
    {
        if (value.getType() != DB::Field::Types::Decimal128)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Field type '{}' for FIXED_LEN_BYTE_ARRAY is not supported", value.getTypeName());
        static_assert(sizeof(Int128) <= sizeof(buf));
        if (static_cast<size_t>(descriptor.type_length()) > sizeof(Int128))
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "descriptor.type_length() = {} , which is > {}, e.g. sizeof(Int128)",
                descriptor.type_length(),
                sizeof(Int128));
        Int128 val = value.get<DB::DecimalField<DB::Decimal128>>().getValue();
        std::reverse(reinterpret_cast<char *>(&val), reinterpret_cast<char *>(&val) + sizeof(val));
        const int offset = sizeof(Int128) - descriptor.type_length();
        memcpy(buf, reinterpret_cast<char *>(&val) + offset, descriptor.type_length());
        return parquet::FixedLenByteArray(buf);
    }
};

// Int32 Int64 Float Double
template <typename DType, typename Col>
struct ConverterNumeric
{
    using From = typename Col::Container::value_type;
    using To = typename DType::c_type;

    const Col & column;
    DB::PODArray<To> buf;

    explicit ConverterNumeric(const DB::ColumnPtr & c) : column(assert_cast<const Col &>(*c)) { }

    const To * getBatch(size_t offset, size_t count)
    {
        if constexpr (sizeof(*column.getData().data()) == sizeof(To))
            return reinterpret_cast<const To *>(column.getData().data() + offset);
        else
        {
            buf.resize(count);
            for (size_t i = 0; i < count; ++i)
                buf[i] = static_cast<To>(column.getData()[offset + i]); // NOLINT
            return buf.data();
        }
    }
};

using ConverterInt32_8 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<Int8>>;
using ConverterInt32_16 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<Int16>>;
using ConverterInt32 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<Int32>>;
using ConverterInt32_u8 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<UInt8>>;
using ConverterInt32_u16 = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<UInt16>>;
using ConverterInt32_u = ConverterNumeric<parquet::Int32Type, DB::ColumnVector<UInt32>>;

using ConverterInt64 = ConverterNumeric<parquet::Int64Type, DB::ColumnVector<Int64>>;
using ConverterInt64_u = ConverterNumeric<parquet::Int64Type, DB::ColumnVector<UInt64>>;

using ConverterDouble = ConverterNumeric<parquet::DoubleType, DB::ColumnVector<Float64>>;

struct ConverterString
{
    const ColumnString & column;
    DB::PODArray<parquet::ByteArray> buf;

    explicit ConverterString(const ColumnPtr & c) : column(assert_cast<const ColumnString &>(*c)) { }

    const parquet::ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
        {
            StringRef s = column.getDataAt(offset + i);
            buf[i] = parquet::ByteArray(static_cast<UInt32>(s.size), reinterpret_cast<const uint8_t *>(s.data));
        }
        return buf.data();
    }
};

/// Like ConverterNumberAsFixedString, but converts to big-endian. Because that's the byte order
/// Parquet uses for decimal types and literally nothing else, for some reason.
template <typename T>
struct ConverterDecimal
{
    const parquet::ColumnDescriptor & descriptor;
    const DB::ColumnDecimal<T> & column;
    DB::PODArray<uint8_t> data_buf;
    DB::PODArray<parquet::FixedLenByteArray> ptr_buf;

    explicit ConverterDecimal(const DB::ColumnPtr & c, const parquet::ColumnDescriptor & desc)
        : descriptor(desc), column(assert_cast<const DB::ColumnDecimal<T> &>(*c))
    {
        if (static_cast<size_t>(descriptor.type_length()) > sizeof(T))
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "descriptor.type_length() = {} , which is > {}, e.g. sizeof(T)",
                descriptor.type_length(),
                sizeof(T));
    }

    const parquet::FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        data_buf.resize(count * sizeof(T));
        ptr_buf.resize(count);
        memcpy(data_buf.data(), reinterpret_cast<const char *>(column.getData().data() + offset), count * sizeof(T));
        const size_t offset_in_buf = sizeof(Int128) - descriptor.type_length();

        for (size_t i = 0; i < count; ++i)
        {
            std::reverse(data_buf.data() + i * sizeof(T), data_buf.data() + (i + 1) * sizeof(T));
            ptr_buf[i].ptr = data_buf.data() + i * sizeof(T) + offset_in_buf;
        }
        return ptr_buf.data();
    }
};

class BaseConverter
{
public:
    virtual ~BaseConverter() = default;
};

template <typename DType>
class ParquetConverter : public BaseConverter
{
protected:
    using T = typename DType::c_type;

public:
    virtual const T * getBatch(size_t offset, size_t count) = 0;
    static std::shared_ptr<ParquetConverter<DType>> Make(const DB::ColumnPtr & c, const parquet::ColumnDescriptor & desc);
};

template <typename DType, typename CONVERT>
class ParquetConverterImpl final : public ParquetConverter<DType>
{
public:
    explicit ParquetConverterImpl(CONVERT && converter) : converter_(std::move(converter)) { }
    const typename ParquetConverter<DType>::T * getBatch(size_t offset, size_t count) override
    {
        return converter_.getBatch(offset, count);
    }

private:
    CONVERT converter_;
};


template <typename DType>
std::shared_ptr<ParquetConverter<DType>> ParquetConverter<DType>::Make(const DB::ColumnPtr & c, const parquet::ColumnDescriptor & desc)
{
    std::shared_ptr<BaseConverter> result;

    using namespace DB;
    switch (DType::type_num)
    {
        case parquet::Type::BOOLEAN:
            break;
        case parquet::Type::INT32:
            switch (c->getDataType())
            {
                case TypeIndex::Int8:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_8>>(ConverterInt32_8(c));
                    break;
                case TypeIndex::Int16:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_16>>(ConverterInt32_16(c));
                    break;
                case TypeIndex::Int32:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32>>(ConverterInt32(c));
                    break;
                case TypeIndex::UInt8:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_u8>>(ConverterInt32_u8(c));
                    break;
                case TypeIndex::UInt16:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_u16>>(ConverterInt32_u16(c));
                    break;
                case TypeIndex::UInt32:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int32Type, ConverterInt32_u>>(ConverterInt32_u(c));
                    break;
                default:
                    break;
            }
            break;
        case parquet::Type::INT64:
            switch (c->getDataType())
            {
                case TypeIndex::Int64:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int64Type, ConverterInt64>>(ConverterInt64(c));
                    break;
                case TypeIndex::UInt64:
                    result = std::make_shared<ParquetConverterImpl<parquet::Int64Type, ConverterInt64_u>>(ConverterInt64_u(c));
                    break;
                default:
                    break;
            }
            break;
        case parquet::Type::INT96:
            break;
        case parquet::Type::FLOAT:
            break;
        case parquet::Type::DOUBLE:
            switch (c->getDataType())
            {
                case TypeIndex::Float64:
                    result = std::make_shared<ParquetConverterImpl<parquet::DoubleType, ConverterDouble>>(ConverterDouble(c));
                    break;
                default:
                    break;
            }
            break;
        case parquet::Type::BYTE_ARRAY:
            switch (c->getDataType())
            {
                case TypeIndex::String:
                    result = std::make_shared<ParquetConverterImpl<parquet::ByteArrayType, ConverterString>>(ConverterString(c));
                    break;
                default:
                    break;
            }
            break;
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            switch (c->getDataType())
            {
                case TypeIndex::Decimal128:
                    result = std::make_shared<ParquetConverterImpl<parquet::FLBAType, ConverterDecimal<Decimal128>>>(
                        ConverterDecimal<Decimal128>(c, desc));
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
    assert(result);
    return std::static_pointer_cast<ParquetConverter<DType>>(result);
}

}
