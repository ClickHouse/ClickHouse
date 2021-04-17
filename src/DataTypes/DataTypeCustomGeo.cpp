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
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nestedDataType()->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nestedDataType()->deserializeAsWholeText(column, istr, settings);
    }

    static DataTypePtr nestedDataType()
    {
        static auto data_type = DataTypePtr(std::make_unique<DataTypeTuple>(
            DataTypes({std::make_unique<DataTypeFloat64>(), std::make_unique<DataTypeFloat64>()})));
        return data_type;
    }
};

class DataTypeCustomRingSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nestedDataType()->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nestedDataType()->deserializeAsWholeText(column, istr, settings);
    }

    static DataTypePtr nestedDataType()
    {
        static auto data_type = DataTypePtr(std::make_unique<DataTypeArray>(DataTypeCustomPointSerialization::nestedDataType()));
        return data_type;
    }
};

class DataTypeCustomPolygonSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nestedDataType()->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nestedDataType()->deserializeAsWholeText(column, istr, settings);
    }

    static DataTypePtr nestedDataType()
    {
        static auto data_type = DataTypePtr(std::make_unique<DataTypeArray>(DataTypeCustomRingSerialization::nestedDataType()));
        return data_type;
    }
};

class DataTypeCustomMultiPolygonSerialization : public DataTypeCustomSimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nestedDataType()->serializeAsText(column, row_num, ostr, settings);
    }

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nestedDataType()->deserializeAsWholeText(column, istr, settings);
    }

    static DataTypePtr nestedDataType()
    {
        static auto data_type = DataTypePtr(std::make_unique<DataTypeArray>(DataTypeCustomPolygonSerialization::nestedDataType()));
        return data_type;
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
