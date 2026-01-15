#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionChar : public IFunction
{
public:
    static constexpr auto name = "char";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionChar>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Number of arguments for function {} can't be {}, should be at least 1",
                            getName(), arguments.size());

        for (const auto & arg : arguments)
        {
            WhichDataType which(arg);
            if (!(which.isInt() || which.isUInt() || which.isFloat()))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of argument of function {}, must be Int, UInt or Float number",
                                arg->getName(), getName());
        }
        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_str = ColumnString::create();
        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        const auto size_per_row = arguments.size();
        out_vec.resize(size_per_row * input_rows_count);
        out_offsets.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
            out_offsets[row] = size_per_row + out_offsets[row - 1];

        Columns columns_holder(arguments.size());
        for (size_t idx = 0; idx < arguments.size(); ++idx)
        {
            // Partial const column
            columns_holder[idx] = arguments[idx].column->convertToFullColumnIfConst();
            const IColumn * column = columns_holder[idx].get();

            if (!(executeNumber<UInt8>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<UInt16>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<UInt32>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<UInt64>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<Int8>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<Int16>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<Int32>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<Int64>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<Float32>(*column, out_vec, idx, input_rows_count, size_per_row)
                  || executeNumber<Float64>(*column, out_vec, idx, input_rows_count, size_per_row)))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                                arguments[idx].column->getName(), getName());
            }
        }

        return col_str;
    }

private:
    template <typename T>
    bool executeNumber(const IColumn & src_data, ColumnString::Chars & out_vec, const size_t & column_idx, const size_t & rows, const size_t & size_per_row) const
    {
        const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);

        if (!src_data_concrete)
            return false;

        for (size_t row = 0; row < rows; ++row)
            out_vec[row * size_per_row + column_idx] = static_cast<char>(src_data_concrete->getInt(row));

        return true;
    }
};

REGISTER_FUNCTION(Char)
{
    FunctionDocumentation::Description description = R"(
Returns a string with length equal to the number of arguments passed where each byte
has the value of the corresponding argument. Accepts multiple arguments of numeric types.

If the value of the argument is out of range of the `UInt8` data type, then it is converted
to `UInt8` with potential rounding and overflow.
        )";
    FunctionDocumentation::Syntax syntax = "char(num1[, num2[, ...]])";
    FunctionDocumentation::Arguments arguments = {
        {"num1[, num2[, num3 ...]]", "Numerical arguments interpreted as integers.", {"(U)Int8/16/32/64", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string of the given bytes.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Basic example",
            "SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello;",
            R"(
┌─hello─┐
│ hello │
└───────┘
              )"
        },
        {
            "Constructing arbitrary encodings",
            R"(
-- You can construct a string of arbitrary encoding by passing the corresponding bytes.
-- for example UTF8
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
            )",
            R"(
┌─hello──┐
│ привет │
└────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionChar>(documentation, FunctionFactory::Case::Insensitive);
}

}
