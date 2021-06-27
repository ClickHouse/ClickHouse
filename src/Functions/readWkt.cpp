#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/geometryConverters.h>
#include <Columns/ColumnString.h>

#include <string>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <class DataTypeName, class Geometry, class Serializer, class NameHolder>
class FunctionReadWkt : public IFunction
{
public:
    explicit FunctionReadWkt() = default;

    static constexpr const char * name = NameHolder::name;

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeString>(arguments[0].get()) == nullptr)
        {
            throw Exception("First argument should be String",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return DataTypeFactory::instance().get(DataTypeName().getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto * column_string = checkAndGetColumn<ColumnString>(arguments[0].column.get());

        Serializer serializer;
        Geometry geometry;

        for (size_t i = 0; i < input_rows_count; i++)
        {
            const auto & str = column_string->getDataAt(i).toString();
            boost::geometry::read_wkt(str, geometry);
            serializer.add(geometry);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReadWkt<DataTypeName, Geometry, Serializer, NameHolder>>();
    }
};

struct ReadWktPointNameHolder
{
    static constexpr const char * name = "readWktPoint";
};

struct ReadWktRingNameHolder
{
    static constexpr const char * name = "readWktRing";
};

struct ReadWktPolygonNameHolder
{
    static constexpr const char * name = "readWktPolygon";
};

struct ReadWktMultiPolygonNameHolder
{
    static constexpr const char * name = "readWktMultiPolygon";
};

void registerFunctionReadWkt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReadWkt<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWktPointNameHolder>>();
    factory.registerFunction<FunctionReadWkt<DataTypeRingName, CartesianRing, RingSerializer<CartesianPoint>, ReadWktRingNameHolder>>();
    factory.registerFunction<FunctionReadWkt<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWktPolygonNameHolder>>();
    factory.registerFunction<FunctionReadWkt<DataTypeMultiPolygonName, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>, ReadWktMultiPolygonNameHolder>>();
}

}
