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
class FunctionReadWKT : public IFunction
{
public:
    explicit FunctionReadWKT() = default;

    static constexpr const char * name = NameHolder::name;

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

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

        for (size_t i = 0; i < input_rows_count; ++i)
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
        return std::make_shared<FunctionReadWKT<DataTypeName, Geometry, Serializer, NameHolder>>();
    }
};

struct ReadWKTPointNameHolder
{
    static constexpr const char * name = "readWKTPoint";
};

struct ReadWKTRingNameHolder
{
    static constexpr const char * name = "readWKTRing";
};

struct ReadWKTPolygonNameHolder
{
    static constexpr const char * name = "readWKTPolygon";
};

struct ReadWKTMultiPolygonNameHolder
{
    static constexpr const char * name = "readWKTMultiPolygon";
};

void registerFunctionReadWKT(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReadWKT<DataTypePointName, CartesianPoint, PointSerializer<CartesianPoint>, ReadWKTPointNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypeRingName, CartesianRing, RingSerializer<CartesianPoint>, ReadWKTRingNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypePolygonName, CartesianPolygon, PolygonSerializer<CartesianPoint>, ReadWKTPolygonNameHolder>>();
    factory.registerFunction<FunctionReadWKT<DataTypeMultiPolygonName, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>, ReadWKTMultiPolygonNameHolder>>();
}

}
