#include <base/types.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

template <typename Impl>
class FunctionFromReadable : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromReadable<Impl>>(); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args
        {
            {"readable_size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeFloat64>();
    }
    

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnFloat64::create();
        auto & res_data = col_to->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {   
            std::string_view str = arguments[0].column->getDataAt(i).toView();
            ReadBufferFromString buf(str);
            // tryReadFloatText does seem to not raise any error when there is leading whitespace so we check it explicitly
            skipWhitespaceIfAny(buf);
            if (buf.getPosition() > 0)
                throw_exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Leading whitespace is not allowed", str);

            Float64 base = 0;
            if (!tryReadFloatTextPrecise(base, buf))    // If we use the default (fast) tryReadFloatText this returns True on garbage input
                throw_exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unable to parse readable size numeric component", str);

            skipWhitespaceIfAny(buf);

            String unit;
            readStringUntilWhitespace(unit, buf);
            if (!buf.eof())
                throw_exception(ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE, "Found trailing characters after readable size string", str);
            boost::algorithm::to_lower(unit);
            Float64 scale_factor = Impl::getScaleFactorForUnit(unit);
            Float64 num_bytes = base * scale_factor;

            res_data.emplace_back(num_bytes);
        }

        return col_to;
    }


private:

    template <typename Arg>
    void throw_exception(const int code, const String & msg, Arg arg) const
    {
        throw Exception(code, "Invalid expression for function {} - {} (\"{}\")", getName(), msg, arg);
    }
};
}
