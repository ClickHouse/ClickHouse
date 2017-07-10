#include <DataTypes/DataTypeUUID.h>

namespace DB
{
    void DataTypeUUID::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        writeText(UUID(static_cast<const ColumnUInt128 &>(column).getData()[row_num]), ostr);
    }

    static void deserializeText(IColumn & column, ReadBuffer & istr)
    {
        UUID x;
        readText(x, istr);
        static_cast<ColumnUInt128 &>(column).getData().push_back(x);
    }

    void DataTypeUUID::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        serializeText(column, row_num, ostr);
    }

    void DataTypeUUID::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
    {
        deserializeText(column, istr);
    }

    void DataTypeUUID::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        writeChar('\'', ostr);
        serializeText(column, row_num, ostr);
        writeChar('\'', ostr);
    }

    void DataTypeUUID::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
    {
        UUID x;
        assertChar('\'', istr);
        readText(x, istr);
        assertChar('\'', istr);
        static_cast<ColumnUInt128 &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
    }

    void DataTypeUUID::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const
    {
        writeChar('"', ostr);
        serializeText(column, row_num, ostr);
        writeChar('"', ostr);
    }

    void DataTypeUUID::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
    {
        UUID x;
        assertChar('"', istr);
        readText(x, istr);
        assertChar('"', istr);
        static_cast<ColumnUInt128 &>(column).getData().push_back(x);
    }

    void DataTypeUUID::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        writeChar('"', ostr);
        serializeText(column, row_num, ostr);
        writeChar('"', ostr);
    }

    void DataTypeUUID::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
    {
        UUID value;
        readCSV(value, istr);
        static_cast<ColumnUInt128 &>(column).getData().push_back(value);
    }
}
