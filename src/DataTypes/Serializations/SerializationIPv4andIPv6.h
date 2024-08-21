#pragma once

#include <base/TypeName.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

template <typename IPv>
class SerializationIP : public SimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override
    {
        writeText(assert_cast<const ColumnVector<IPv> &>(column).getData()[row_num], ostr);
    }
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override
    {
        IPv x;
        readText(x, istr);

        if (whole && !istr.eof())
            throwUnexpectedDataAfterParsedValue(column, istr, settings, TypeName<IPv>.data());

        assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    }
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings, false);
    }
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        writeChar('\'', ostr);
        serializeText(column, row_num, ostr, settings);
        writeChar('\'', ostr);
    }
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override
    {
        IPv x;
        assertChar('\'', istr);
        readText(x, istr);
        assertChar('\'', istr);
        assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
    }
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        writeChar('"', ostr);
        serializeText(column, row_num, ostr, settings);
        writeChar('"', ostr);
    }
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        IPv x;
        assertChar('"', istr);
        readText(x, istr);
        /// this code looks weird, but we want to throw specific exception to match original behavior...
        if (istr.eof())
            assertChar('"', istr);
        if (*istr.position() != '"')
            throwUnexpectedDataAfterParsedValue(column, istr, settings, TypeName<IPv>.data());
        istr.ignore();

        assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    }
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        writeChar('"', ostr);
        serializeText(column, row_num, ostr, settings);
        writeChar('"', ostr);
    }
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &/* settings*/) const override
    {
        IPv value;
        readCSV(value, istr);

        assert_cast<ColumnVector<IPv> &>(column).getData().push_back(value);
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override
    {
        IPv x = field.get<IPv>();
        writeBinary(x, ostr);
    }
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override
    {
        IPv x;
        readBinary(x.toUnderType(), istr);
        field = NearestFieldType<IPv>(x);
    }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override
    {
        writeBinary(assert_cast<const ColumnVector<IPv> &>(column).getData()[row_num], ostr);
    }
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override
    {
        IPv x;
        readBinary(x.toUnderType(), istr);
        assert_cast<ColumnVector<IPv> &>(column).getData().push_back(x);
    }
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override
    {
        const typename ColumnVector<IPv>::Container & x = typeid_cast<const ColumnVector<IPv> &>(column).getData();

        size_t size = x.size();

        if (limit == 0 || offset + limit > size)
            limit = size - offset;

        if (limit)
            ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(IPv) * limit);
    }
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const override
    {
        typename ColumnVector<IPv>::Container & x = typeid_cast<ColumnVector<IPv> &>(column).getData();
        size_t initial_size = x.size();
        x.resize(initial_size + limit);
        size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(IPv) * limit);
        x.resize(initial_size + size / sizeof(IPv));
    }
};

using SerializationIPv4 = SerializationIP<IPv4>;
using SerializationIPv6 = SerializationIP<IPv6>;

}
