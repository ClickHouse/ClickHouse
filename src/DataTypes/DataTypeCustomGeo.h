#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeCustomSimpleTextSerialization.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

class DataTypeCustomPointSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    static DataTypePtr nestedDataType();
};


class DataTypeCustomRingSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    static DataTypePtr nestedDataType();
};

class DataTypeCustomPolygonSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    static DataTypePtr nestedDataType();
};

class DataTypeCustomMultiPolygonSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    static DataTypePtr nestedDataType();
};

}
