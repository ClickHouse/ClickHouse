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
extern const int BAD_ARGUMENT;
}

template <class DataType, class Geometry, class Serializer>
class FunctionReadWkt : public IFunction
{
public:
    explicit FunctionReadWkt() {}

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

        return DataType::nestedDataType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto column_string = checkAndGetColumn<ColumnString>(arguments[0].column.get());

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
};

class FunctionReadWktPoint : public FunctionReadWkt<DataTypeCustomPointSerialization, CartesianPoint, PointSerializer<CartesianPoint>>
{
public:
    static inline const char * name = "readWktPoint";
    String getName() const override
    {
        return name;
    }
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReadWktPoint>();
    }
};

class FunctionReadWktPolygon : public FunctionReadWkt<DataTypeCustomPolygonSerialization, CartesianPolygon, PolygonSerializer<CartesianPoint>>
{
public:
    static inline const char * name = "readWktPolygon";
    String getName() const override
    {
        return name;
    }
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReadWktPolygon>();
    }
};

class FunctionReadWktMultiPolygon : public FunctionReadWkt<DataTypeCustomMultiPolygonSerialization, CartesianMultiPolygon, MultiPolygonSerializer<CartesianPoint>>
{
public:
    static inline const char * name = "readWktMultiPolygon";
    String getName() const override
    {
        return name;
    }
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReadWktMultiPolygon>();
    }
};

void registerFunctionReadWkt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReadWktPoint>();
    factory.registerFunction<FunctionReadWktPolygon>();
    factory.registerFunction<FunctionReadWktMultiPolygon>();
}

}
