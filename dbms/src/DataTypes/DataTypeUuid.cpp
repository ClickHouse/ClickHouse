#include <DataTypes/DataTypeUuid.h>

namespace DB
{
    void DataTypeUuid::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        writeText(Uuid(static_cast<const ColumnUuid &>(column).getData()[row_num]), ostr);
    }

    static void deserializeText(IColumn & column, ReadBuffer & istr)
    {
        Uuid  x;
        readText(x, istr);
        static_cast<ColumnUuid &>(column).getData().push_back(x);
    }

    void DataTypeUuid::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        serializeText(column, row_num, ostr);
    }

    void DataTypeUuid::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
    {
        deserializeText(column, istr);
    }

    void DataTypeUuid::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        writeChar('\'', ostr);
        serializeText(column, row_num, ostr);
        writeChar('\'', ostr);
    }

    void DataTypeUuid::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
    {
        Uuid x;
        assertChar('\'', istr);
        readText(x, istr);
        assertChar('\'', istr);
        static_cast<ColumnUuid &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
    }

    void DataTypeUuid::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, bool) const
    {
        writeChar('"', ostr);
        serializeText(column, row_num, ostr);
        writeChar('"', ostr);
    }

    void DataTypeUuid::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
    {
        Uuid x;
        assertChar('"', istr);
        readText(x, istr);
        assertChar('"', istr);
        static_cast<ColumnUuid &>(column).getData().push_back(x);
    }

    void DataTypeUuid::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
    {
        writeChar('"', ostr);
        serializeText(column, row_num, ostr);
        writeChar('"', ostr);
    }

    void DataTypeUuid::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
    {
        Uuid value;
        readCSV(value, istr);
        static_cast<ColumnUuid &>(column).getData().push_back(value);
    }
}
