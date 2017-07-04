#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** The base class for data types that do not support serialization and deserialization,
  *  but arise only as an intermediate result of the calculations.
  *
  * That is, this class is used just to distinguish the corresponding data type from the others.
  */
class IDataTypeDummy : public IDataType
{
private:
    bool notForTables() const override
    {
        return true;
    }

    void throwNoSerialization() const
    {
        throw Exception("Serialization is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

public:
    void serializeBinary(const Field & field, WriteBuffer & ostr) const override                       { throwNoSerialization(); }
    void deserializeBinary(Field & field, ReadBuffer & istr) const override                            { throwNoSerialization(); }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override    { throwNoSerialization(); }
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override                         { throwNoSerialization(); }

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr,
        size_t offset, size_t limit) const override                                                    { throwNoSerialization(); }

    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override { throwNoSerialization(); }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override           { throwNoSerialization(); }

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override    { throwNoSerialization(); }
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override                         { throwNoSerialization(); }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override     { throwNoSerialization(); }
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override                          { throwNoSerialization(); }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override { throwNoSerialization(); }
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override                            { throwNoSerialization(); }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override        { throwNoSerialization(); }
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override       { throwNoSerialization(); }

    ColumnPtr createColumn() const override
    {
        throw Exception("Method createColumn() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr createConstColumn(size_t size, const Field & field) const override
    {
        throw Exception("Method createConstColumn() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    Field getDefault() const override
    {
        throw Exception("Method getDefault() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
};

}

