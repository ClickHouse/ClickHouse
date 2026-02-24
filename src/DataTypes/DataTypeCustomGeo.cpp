#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

void registerDataTypeDomainGeo(DataTypeFactory & factory)
{
    // Custom type for point represented as its coordinates stored as Tuple(Float64, Float64)
    factory.registerSimpleDataTypeCustom("Point", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Tuple(Float64, Float64)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypePointName>()));
    });

    // Custom type for simple line which consists from several segments.
    factory.registerSimpleDataTypeCustom("LineString", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Point)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeLineStringName>()));
    });

    // Custom type for multiple lines stored as Array(LineString)
    factory.registerSimpleDataTypeCustom("MultiLineString", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(LineString)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeMultiLineStringName>()));
    });

    // Custom type for simple polygon without holes stored as Array(Point)
    factory.registerSimpleDataTypeCustom("Ring", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Point)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeRingName>()));
    });

    // Custom type for polygon with holes stored as Array(Ring)
    // First element of outer array is outer shape of polygon and all the following are holes
    factory.registerSimpleDataTypeCustom("Polygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Ring)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypePolygonName>()));
    });

    // Custom type for multiple polygons with holes stored as Array(Polygon)
    factory.registerSimpleDataTypeCustom("MultiPolygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Polygon)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeMultiPolygonName>()));
    });
}

}
