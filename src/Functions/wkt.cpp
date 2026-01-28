#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Columns/ColumnString.h>

#include <string>
#include <memory>

namespace DB
{

namespace
{

class FunctionWkt : public IFunction
{
public:
    static inline const char * name = "wkt";

    explicit FunctionWkt() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionWkt>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /*
    * Functions like recursiveRemoveLowCardinality don't pay enough attention to custom types and just erase
    * the information about it during type conversions.
    * While it is a big problem the quick solution would be just to disable default low cardinality implementation
    * because it doesn't make a lot of sense for geo types.
    */
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnString::create();

        callOnGeometryDataType<CartesianPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            auto figures = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::stringstream str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                str.exceptions(std::ios::failbit);
                str << boost::geometry::wkt(figures[i]);
                std::string serialized = str.str();
                res_column->insertData(serialized.c_str(), serialized.size());
            }
        }
        );

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

}

REGISTER_FUNCTION(Wkt)
{
    factory.registerFunction<FunctionWkt>();
}

}
