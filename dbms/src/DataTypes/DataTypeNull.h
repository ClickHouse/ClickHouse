#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// Data type which represents a single NULL value. It is the type
/// associated to a constant column that contains only NULL values,
/// namely ColumnNull, which arises when a NULL is specified as a
/// column in any query.
class DataTypeNull final : public IDataType
{
public:
    using FieldType = Null;

public:
    String getName() const override
    {
        return "Null";
    }

    bool isNull() const override
    {
        return true;
    }

    bool notForTables() const override
    {
        return true;
    }

    DataTypePtr clone() const override
    {
        return std::make_shared<DataTypeNull>();
    }

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    ColumnPtr createColumn() const override;
    ColumnPtr createConstColumn(size_t size, const Field & field) const override;

    Field getDefault() const override
    {
        return Null();
    }

    size_t getSizeOfField() const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
};

}
