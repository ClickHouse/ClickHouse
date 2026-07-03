#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Columns/ColumnsNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Common/UTF8Helpers.h>
#include <Common/assert_cast.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 function_visible_width_behavior;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** visibleWidth(x) - calculates the approximate width when outputting the value in a text form to the console.
  * In fact it calculate the number of Unicode code points.
  * It does not support zero width and full width characters, combining characters, etc.
  */
class FunctionVisibleWidth : public IFunction
{
private:
    UInt64 behavior;

public:
    static constexpr auto name = "visibleWidth";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionVisibleWidth>(context);
    }

    explicit FunctionVisibleWidth(ContextPtr context) { behavior = context->getSettingsRef()[Setting::function_visible_width_behavior]; }

    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Execute the function on the columns.
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src = arguments[0];

        auto res_col = ColumnUInt64::create(input_rows_count);
        auto & res_data = assert_cast<ColumnUInt64 &>(*res_col).getData();

        /// For simplicity reasons, the function is implemented by serializing into temporary buffer.

        String tmp;
        FormatSettings format_settings;
        auto serialization = src.type->getDefaultSerialization();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            {
                WriteBufferFromString out(tmp);
                serialization->serializeText(*src.column, i, out, format_settings);
            }

            switch (behavior)
            {
                case 0:
                    res_data[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(tmp.data()), tmp.size());
                    break;
                case 1:
                    res_data[i] = UTF8::computeWidth(reinterpret_cast<const UInt8 *>(tmp.data()), tmp.size());
                    break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported value {} of the `function_visible_width_behavior` setting", behavior);
            }
        }

        return res_col;
    }
};


REGISTER_FUNCTION(VisibleWidth)
{
    factory.registerFunction<FunctionVisibleWidth>();
}

}
