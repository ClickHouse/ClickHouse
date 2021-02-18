#include <DataTypes/DataTypeCustomGeo.h>

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
    const auto point_data_type = std::make_shared<const DataTypeTuple>(
        DataTypes{std::make_shared<const DataTypeFloat64>(), std::make_shared<const DataTypeFloat64>()}
    );

    const auto ring_data_type = std::make_shared<const DataTypeArray>(DataTypeCustomPointSerialization::nestedDataType());

    const auto polygon_data_type = std::make_shared<const DataTypeArray>(DataTypeCustomRingSerialization::nestedDataType());

    const auto multipolygon_data_type = std::make_shared<const DataTypeArray>(DataTypeCustomPolygonSerialization::nestedDataType());
}


void DataTypeCustomPointSerialization::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nestedDataType()->serializeAsText(column, row_num, ostr, settings);
}

void DataTypeCustomPointSerialization::deserializeText(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nestedDataType()->deserializeAsWholeText(column, istr, settings);
}

DataTypePtr DataTypeCustomPointSerialization::nestedDataType()
{
    return point_data_type;
}

void DataTypeCustomRingSerialization::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nestedDataType()->serializeAsText(column, row_num, ostr, settings);
}

void DataTypeCustomRingSerialization::deserializeText(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nestedDataType()->deserializeAsWholeText(column, istr, settings);
}

DataTypePtr DataTypeCustomRingSerialization::nestedDataType()
{
    return ring_data_type;
}

void DataTypeCustomPolygonSerialization::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nestedDataType()->serializeAsText(column, row_num, ostr, settings);
}

void DataTypeCustomPolygonSerialization::deserializeText(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nestedDataType()->deserializeAsWholeText(column, istr, settings);
}

DataTypePtr DataTypeCustomPolygonSerialization::nestedDataType()
{
    return polygon_data_type;
}

void DataTypeCustomMultiPolygonSerialization::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nestedDataType()->serializeAsText(column, row_num, ostr, settings);
}

void DataTypeCustomMultiPolygonSerialization::deserializeText(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nestedDataType()->deserializeAsWholeText(column, istr, settings);
}

DataTypePtr DataTypeCustomMultiPolygonSerialization::nestedDataType()
{
    return multipolygon_data_type;
}

void registerDataTypeDomainGeo(DataTypeFactory & factory)
{
    // Custom type for point represented as its coordinates stored as Tuple(Float64, Float64)
    factory.registerSimpleDataTypeCustom("Point", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Tuple(Float64, Float64)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("Point"), std::make_unique<DataTypeCustomPointSerialization>()));
    });

    // Custom type for simple polygon without holes stored as Array(Point)
    factory.registerSimpleDataTypeCustom("Ring", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Point)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("Ring"), std::make_unique<DataTypeCustomRingSerialization>()));
    });

    // Custom type for polygon with holes stored as Array(Ring)
    // First element of outer array is outer shape of polygon and all the following are holes
    factory.registerSimpleDataTypeCustom("Polygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Ring)"),
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
