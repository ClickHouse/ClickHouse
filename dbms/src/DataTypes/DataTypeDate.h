#pragma once

#include <DataTypes/DataTypeNumberBase.h>


namespace DB
{

class DataTypeDate final : public DataTypeNumberBase<UInt16>
{
public:
    const char * getFamilyName() const override { return "Date"; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override;

    bool canBeUsedAsVersion() const override { return true; }
    bool isDateOrDateTime() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType & rhs) const override;
};

}
