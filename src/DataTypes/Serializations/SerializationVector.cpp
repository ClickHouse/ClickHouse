#include <DataTypes/Serializations/SerializationVector.h>
#include <DataTypes/Serializations/SerializationFixedString.h>

#include <Columns/ColumnDenseVector.h>
#include <Columns/ColumnFixedString.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/assert_cast.h>

#include <base/BFloat16.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


SerializationPtr SerializationVector::create(size_t element_size_, size_t dimension_)
{
    auto nested = SerializationFixedString::create(element_size_ * dimension_);
    return std::shared_ptr<SerializationVector>(new SerializationVector(nested, element_size_, dimension_));
}

template <typename Func>
void SerializationVector::dispatchByElementSize(Func && func) const
{
    if (element_size == 2)
        func.template operator()<BFloat16>();
    else if (element_size == 4)
        func.template operator()<Float32>();
    else if (element_size == 8)
        func.template operator()<Float64>();
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported element size for Vector: {}. Only 2, 4 and 8 are supported", element_size);
}

static const ColumnFixedString & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnDenseVector &>(column).getFixedStringData();
}

static ColumnFixedString & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnDenseVector &>(column).getFixedStringData();
}

void SerializationVector::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}

void SerializationVector::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}

void SerializationVector::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(extractNestedColumn(column), row_num, ostr, settings);
}

void SerializationVector::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(extractNestedColumn(column), istr, settings);
}

void SerializationVector::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    nested->serializeBinaryBulk(extractNestedColumn(column), ostr, offset, limit);
}

void SerializationVector::deserializeBinaryBulk(
    IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    nested->deserializeBinaryBulk(extractNestedColumn(column), istr, rows_offset, limit, avg_value_size_hint);
}

void SerializationVector::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const std::string_view bytes = extractNestedColumn(column).getDataAt(row_num);

    writeChar('[', ostr);
    dispatchByElementSize(
        [&]<typename T>()
        {
            const T * data = reinterpret_cast<const T *>(bytes.data());
            for (size_t i = 0; i < dimension; ++i)
            {
                if (i != 0)
                    writeChar(',', ostr);
                writeText(data[i], ostr);
            }
        });
    writeChar(']', ostr);
}

void SerializationVector::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    auto & chars = extractNestedColumn(column).getChars();
    const size_t value_size = element_size * dimension;
    const size_t old_size = chars.size();

    /// Reserve space for one row; roll back on any parse error so the column is left consistent.
    chars.resize(old_size + value_size);

    try
    {
        assertChar('[', istr);
        skipWhitespaceIfAny(istr);

        dispatchByElementSize(
            [&]<typename T>()
            {
                T * data = reinterpret_cast<T *>(&chars[old_size]);
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
                    data[i] = value;
                }
            });

        skipWhitespaceIfAny(istr);
        assertChar(']', istr);

        if (whole && !istr.eof())
            throwUnexpectedDataAfterParsedValue(column, istr, settings, "Vector");
    }
    catch (...)
    {
        chars.resize(old_size);
        throw;
    }
}

}
