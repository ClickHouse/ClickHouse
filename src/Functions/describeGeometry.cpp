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

class FunctionDescribeGeometry : public IFunction
{
public:
    explicit FunctionDescribeGeometry() = default;

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    static inline const char * name = "describeGeometry";


    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeString>(arguments[0].get()) == nullptr)
        {
            throw Exception("First argument should be String",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto * column_string = checkAndGetColumn<ColumnString>(arguments[0].column.get());

        CartesianPoint point;
        Ring<CartesianPoint> ring;
        Polygon<CartesianPoint> polygon;
        MultiPolygon<CartesianPoint> multipolygon;

        auto result = ColumnUInt8::create();
        auto & result_array = result->getData();

        result_array.reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; i++)
        {
            const auto & str = column_string->getDataAt(i).toString();

            try
            {
                boost::geometry::read_wkt(str, point);
                result_array.emplace_back(0);
                continue;
            }
            catch (boost::geometry::read_wkt_exception &)
            { 
            }

            try
            {
                boost::geometry::read_wkt(str, ring);
                result_array.emplace_back(1);
                continue;
            }
            catch (boost::geometry::read_wkt_exception &)
            { 
            }


            try
            {
                boost::geometry::read_wkt(str, polygon);
                result_array.emplace_back(2);
                continue;
            }
            catch (boost::geometry::read_wkt_exception &)
            { 
            }


            try
            {
                boost::geometry::read_wkt(str, multipolygon);
                result_array.emplace_back(3);
                continue;
            }
            catch (boost::geometry::read_wkt_exception &)
            { 
            }

            throw Exception("Unknown geometry format", ErrorCodes::BAD_ARGUMENTS);
        }

        return result;
    }

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionDescribeGeometry>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


void registerFunctionDescribeGeometry(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDescribeGeometry>();
}

}
