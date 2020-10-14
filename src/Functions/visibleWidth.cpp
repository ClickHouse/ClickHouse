#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Columns/ColumnsNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Common/UTF8Helpers.h>
#include <Common/assert_cast.h>


namespace DB
{

/** visibleWidth(x) - calculates the approximate width when outputting the value in a text form to the console.
  * In fact it calculate the number of Unicode code points.
  * It does not support zero width and full width characters, combining characters, etc.
  */
class FunctionVisibleWidth : public IFunction
{
public:
    static constexpr auto name = "visibleWidth";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionVisibleWidth>();
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

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
    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        auto & src = columns[arguments[0]];
        size_t size = input_rows_count;

        auto res_col = ColumnUInt64::create(size);
        auto & res_data = assert_cast<ColumnUInt64 &>(*res_col).getData();

        /// For simplicity reasons, function is implemented by serializing into temporary buffer.

        String tmp;
        FormatSettings format_settings;
        for (size_t i = 0; i < size; ++i)
        {
            {
                WriteBufferFromString out(tmp);
                src.type->serializeAsText(*src.column, i, out, format_settings);
            }

            res_data[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(tmp.data()), tmp.size());
        }

        columns[result].column = std::move(res_col);
    }
};


void registerFunctionVisibleWidth(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVisibleWidth>();
}

}
