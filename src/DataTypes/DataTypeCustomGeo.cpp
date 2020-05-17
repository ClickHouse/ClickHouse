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

namespace
{

class DataTypeCustomPointSerialization : public DataTypeCustomSimpleTextSerialization
{
private:
    DataTypePtr tuple;

public:
    DataTypeCustomPointSerialization() : tuple(std::make_unique<DataTypeTuple>(
        DataTypes({std::make_unique<DataTypeFloat64>(), std::make_unique<DataTypeFloat64>()})))
    {}

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        tuple->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        tuple->deserializeAsWholeText(column, istr, settings);
    }
};

class DataTypeCustomPolygonSerialization : public DataTypeCustomSimpleTextSerialization
{
private:
    DataTypePtr array;

public:
    DataTypeCustomPolygonSerialization() : array(std::make_unique<DataTypeArray>(std::make_unique<DataTypeArray>(std::make_unique<DataTypeTuple>(
            DataTypes({std::make_unique<DataTypeFloat64>(), std::make_unique<DataTypeFloat64>()})))))
    {}

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        array->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        array->deserializeAsWholeText(column, istr, settings);
    }
};

class DataTypeCustomMultiPolygonSerialization : public DataTypeCustomSimpleTextSerialization
{
private:
    DataTypePtr array;

public:
    DataTypeCustomMultiPolygonSerialization() : array(
        std::make_unique<DataTypeArray>(std::make_unique<DataTypeArray>(
        std::make_unique<DataTypeArray>(std::make_unique<DataTypeTuple>(
            DataTypes({std::make_unique<DataTypeFloat64>(), std::make_unique<DataTypeFloat64>()}))))))
    {}

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        array->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        array->deserializeAsWholeText(column, istr, settings);
    }
};

}

void registerDataTypeDomainGeo(DataTypeFactory & factory)
{
    // Custom type for point represented as its coordinates stored as Tuple(Float64, Float64)
    factory.registerSimpleDataTypeCustom("Point", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Tuple(Float64, Float64)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("Point"), std::make_unique<DataTypeCustomPointSerialization>()));
    });

    // Custom type for polygon with holes stored as Array(Array(Point))
    // Each element of outer array represents a simple polygon without holes stored as array of points
    // First element of outer array is outer shape of polygon and all the following are holes
    factory.registerSimpleDataTypeCustom("Polygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Array(Point))"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("Polygon"), std::make_unique<DataTypeCustomPolygonSerialization>()));
    });

    // Custom type for multiple polygons with holes stored as Array(Polygon)
    factory.registerSimpleDataTypeCustom("MultiPolygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Polygon)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("MultiPolygon"), std::make_unique<DataTypeCustomMultiPolygonSerialization>()));
    });
}

}
