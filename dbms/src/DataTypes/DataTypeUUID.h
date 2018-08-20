#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class DataTypeUUID final : public DataTypeNumberBase<UInt128>
{

public:
    const char * getFamilyName() const override { return "UUID"; }
    TypeIndex getTypeId() const override { return TypeIndex::UUID; }

    bool equals(const IDataType & rhs) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
};

}
