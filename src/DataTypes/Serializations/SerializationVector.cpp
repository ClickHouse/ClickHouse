#include <DataTypes/Serializations/SerializationVector.h>

#include <Columns/ColumnDenseVector.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include <base/BFloat16.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
extern const int TOO_LARGE_ARRAY_SIZE;
extern const int TYPE_MISMATCH;
}


UInt128 SerializationVector::getHash(TypeIndex element_type_, size_t dimension_)
{
    SipHash hash;
    hash.update("Vector");
    hash.update(element_type_);
    hash.update(dimension_);
    return hash.get128();
}

SerializationPtr SerializationVector::create(TypeIndex element_type_, size_t dimension_)
{
    return ISerialization::pooled(getHash(element_type_, dimension_), [=] { return new SerializationVector(element_type_, dimension_); });
}


void SerializationVector::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const Array & array = field.safeGet<Array>();
    if (array.size() != dimension)
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Cannot serialize array of size {} as a Vector with dimension {}",
            array.size(),
            dimension);

    ColumnDenseVector::dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        for (size_t i = 0; i < dimension; ++i)
        {
            T value = static_cast<T>(array[i].safeGet<T>());
            writeBinaryLittleEndian(value, ostr);
        }
    });
}

void SerializationVector::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    field = Array();
    Array & array = field.safeGet<Array>();
    array.reserve(dimension);

    ColumnDenseVector::dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        for (size_t i = 0; i < dimension; ++i)
        {
            T value;
            readBinaryLittleEndian(value, istr);
            array.push_back(static_cast<NearestFieldType<T>>(value));
        }
    });
}

void SerializationVector::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto value = assert_cast<const ColumnDenseVector &>(column).getDataAt(row_num);
    ostr.write(value.data(), value.size());
}

void SerializationVector::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    auto & column_vector = assert_cast<ColumnDenseVector &>(column);

    ColumnDenseVector::dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = column_vector.getTypedData<T>();
        const size_t old_size = data.size();
        data.resize(old_size + dimension);
        try
        {
            istr.readStrict(reinterpret_cast<char *>(&data[old_size]), dimension * sizeof(T));
        }
        catch (...)
        {
            data.resize(old_size);
            throw;
        }
    });
}

void SerializationVector::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const auto & column_vector = assert_cast<const ColumnDenseVector &>(column);
    const size_t value_size = column_vector.getValueSize();
    const size_t size = column_vector.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(column_vector.getRawData().data() + offset * value_size, limit * value_size);
}

void SerializationVector::deserializeBinaryBulk(
    IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double /*avg_value_size_hint*/) const
{
    auto & column_vector = assert_cast<ColumnDenseVector &>(column);
    const size_t value_size = column_vector.getValueSize();

    size_t skipped_bytes;
    if (unlikely(__builtin_mul_overflow(rows_offset, value_size, &skipped_bytes)))
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Deserializing Vector will lead to overflow");
    istr.ignore(skipped_bytes);

    size_t max_bytes;
    if (unlikely(__builtin_mul_overflow(limit, value_size, &max_bytes)))
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Deserializing Vector will lead to overflow");

    ColumnDenseVector::dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = column_vector.getTypedData<T>();
        const size_t initial_size = data.size();

        data.resize(initial_size + limit * dimension);
        const size_t read_bytes = istr.readBig(reinterpret_cast<char *>(&data[initial_size]), max_bytes);

        if (read_bytes % value_size != 0)
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data of type Vector. Bytes read: {}. Value size: {}.",
                read_bytes,
                value_size);

        data.resize(initial_size + read_bytes / sizeof(T));
    });
}

void SerializationVector::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto & column_vector = assert_cast<const ColumnDenseVector &>(column);

    writeChar('[', ostr);
    ColumnDenseVector::dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        const auto & data = column_vector.getTypedData<T>();
        const size_t offset = row_num * dimension;
        for (size_t i = 0; i < dimension; ++i)
        {
            if (i != 0)
                writeChar(',', ostr);
            writeText(data[offset + i], ostr);
        }
    });
    writeChar(']', ostr);
}

void SerializationVector::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// There is no good way to serialize an array-like value in CSV. Therefore, we serialize it into a
    /// string, and then write the resulting string in CSV (the same way as SerializationArray).
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}

void SerializationVector::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings, true);
}

void SerializationVector::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    auto & column_vector = assert_cast<ColumnDenseVector &>(column);

    ColumnDenseVector::dispatchByElementType(element_type, [&](auto tag)
    {
        using T = decltype(tag);
        auto & data = column_vector.getTypedData<T>();
        const size_t old_size = data.size();

        /// Reserve space for one row; roll back on any parse error so the column is left consistent.
        data.resize(old_size + dimension);

        try
        {
            assertChar('[', istr);
            skipWhitespaceIfAny(istr);

            for (size_t i = 0; i < dimension; ++i)
            {
                if (i != 0)
                {
                    skipWhitespaceIfAny(istr);
                    assertChar(',', istr);
                    skipWhitespaceIfAny(istr);
                }
                T value;
                readText(value, istr);
                data[old_size + i] = value;
            }

            skipWhitespaceIfAny(istr);
            assertChar(']', istr);

            if (whole && !istr.eof())
                throwUnexpectedDataAfterParsedValue(column, istr, settings, "Vector");
        }
        catch (...)
        {
            data.resize(old_size);
            throw;
        }
    });
}

}
