#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>
#include <IO/WriteHelpers.h>

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

        const auto size_per_row = arguments.size() + 1;
        out_vec.resize(size_per_row * input_rows_count);
        out_offsets.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            out_offsets[row] = size_per_row + out_offsets[row - 1];
            out_vec[row * size_per_row + size_per_row - 1] = '\0';
        }

        Columns columns_holder(arguments.size());
        for (size_t idx = 0; idx < arguments.size(); ++idx)
        {
            //partial const column
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
    factory.registerFunction<FunctionChar>({}, FunctionFactory::Case::Insensitive);
}

}
