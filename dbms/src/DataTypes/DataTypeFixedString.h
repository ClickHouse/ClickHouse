#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}


class DataTypeFixedString final : public IDataType
{
private:
    size_t n;

public:
    DataTypeFixedString(size_t n_) : n(n_)
    {
        if (n == 0)
            throw Exception("FixedString size must be positive", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    std::string getName() const override;

    DataTypePtr clone() const override
    {
        return std::make_shared<DataTypeFixedString>(n);
    }

    size_t getN() const
    {
        return n;
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    ColumnPtr createColumn() const override;
    ColumnPtr createConstColumn(size_t size, const Field & field) const override;

    Field getDefault() const override
    {
        return String();
    }
};

}
