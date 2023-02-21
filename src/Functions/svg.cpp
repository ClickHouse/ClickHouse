#include <DataTypes/DataTypeString.h>
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
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionSvg : public IFunction
{
public:
    static inline const char * name = "svg";

    explicit FunctionSvg() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionSvg>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 2)
        {
            throw Exception("Too many arguments", ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }
        else if (arguments.empty())
        {
            throw Exception("Too few arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);
        }
        else if (arguments.size() == 2 && checkAndGetDataType<DataTypeString>(arguments[1].get()) == nullptr)
        {
            throw Exception("Second argument should be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnString::create();
        bool has_style = arguments.size() > 1;
        ColumnPtr style;
        if (has_style)
            style = arguments[1].column;

        callOnGeometryDataType<CartesianPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            auto figures = Converter::convert(arguments[0].column->convertToFullColumnIfConst());

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::stringstream str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                boost::geometry::correct(figures[i]);
                str << boost::geometry::svg(figures[i], has_style ? style->getDataAt(i).toString() : "");
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

REGISTER_FUNCTION(Svg)
{
    factory.registerFunction<FunctionSvg>();
    factory.registerAlias("SVG", "svg");
}

}
