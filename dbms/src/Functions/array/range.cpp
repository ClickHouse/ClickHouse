#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionRange : public IFunction
{
public:
    static constexpr auto name = "range";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRange>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 3 || arguments.empty())
        {
            throw Exception{"Function " + getName() + " needs [1-3] argument; passed "
                            + std::to_string(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
        }

        for (const auto & arg : arguments)
        {
            if (!isUnsignedInteger(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        return std::make_shared<DataTypeArray>(arguments.size() == 3 ? arguments[1] : arguments.back());
    }

    template <typename Start, typename End, typename Step>
    bool executeStartEndStep(Block & block, const IColumn * start_col, const IColumn * end_col, const IColumn * step_col, const size_t result)
    {
        static constexpr size_t max_elements = 100'000'000;

        auto start_column = checkAndGetColumn<ColumnVector<Start>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<End>>(end_col);
        auto step_column = checkAndGetColumn<ColumnVector<Step>>(step_col);

        if (!start_column || !end_column || !step_column)
        {
            return false;
        }

        const auto & start_data = start_column->getData();
        const auto & end_start = end_column->getData();
        const auto & step_data = step_column->getData();

        size_t total_values = 0;
        size_t pre_values = 0;

        for (size_t row_idx = 0, rows = end_column->size(); row_idx < rows; ++row_idx)
        {
            pre_values += start_data[row_idx] >= end_start[row_idx] ? 0
                            : (end_start[row_idx] -start_data[row_idx] - 1) / (step_data[row_idx]) + 1;

            if (pre_values < total_values)
                throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            total_values = pre_values;
            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};
        }

        auto data_col = ColumnVector<End>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        IColumn::Offset offset{};
        for (size_t row_idx = 0, rows = end_column->size(); row_idx < rows; ++row_idx)
        {
            for (size_t st = start_data[row_idx], ed = end_start[row_idx]; st < ed; st += step_data[row_idx])
                out_data[offset++] = st;

            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }

    template <typename Start, typename End>
    bool executeStartEnd(Block & block, const IColumn * start_col, const IColumn * end_col, const IColumn * step_col, const size_t result)
    {
        return executeStartEndStep<Start, End, UInt8>(block, start_col, end_col, step_col,result)
               || executeStartEndStep<Start, End, UInt16>(block, start_col, end_col, step_col,result)
               || executeStartEndStep<Start, End, UInt32>(block, start_col, end_col, step_col,result)
               || executeStartEndStep<Start, End, UInt64>(block, start_col, end_col, step_col,result);
    }

    template <typename Start>
    bool executeStart(Block & block, const IColumn * start_col, const IColumn * end_col, const IColumn * step_col, const size_t result)
    {
        return executeStartEnd<Start, UInt8>(block, start_col, end_col, step_col, result)
               || executeStartEnd<Start, UInt16>(block, start_col, end_col, step_col, result)
               || executeStartEnd<Start, UInt32>(block, start_col, end_col, step_col, result)
               || executeStartEnd<Start, UInt64>(block, start_col, end_col, step_col, result);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        Columns columns_holder(3);
        ColumnRawPtrs columns(3);
        size_t idx = 0;
        size_t rows =  block.getByPosition(arguments[0]).column.get()->size();

        // for start column, default to 0
        if (arguments.size() == 1)
        {
            columns_holder[idx] = DataTypeUInt8().createColumnConst(rows, 0)->convertToFullColumnIfConst();
            columns[idx] = columns_holder[idx].get();
            idx ++;
        }

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            columns_holder[idx] = block.getByPosition(arguments[i]).column->convertToFullColumnIfConst();
            columns[idx] = columns_holder[idx].get();
            idx ++;
        }

        // for step column, defaults to 1
        if (arguments.size() <= 2)
        {
            columns_holder[idx] = DataTypeUInt8().createColumnConst(rows, 1)->convertToFullColumnIfConst();
            columns[idx] = columns_holder[idx].get();
        }

        if (!executeStart<UInt8>(block, columns[0], columns[1], columns[2], result) &&
            !executeStart<UInt16>(block, columns[0], columns[1], columns[2], result) &&
            !executeStart<UInt32>(block, columns[0], columns[1], columns[2], result) &&
            !executeStart<UInt64>(block, columns[0], columns[1], columns[2], result))
        {
            throw Exception{"Illegal columns " + columns[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }

};


void registerFunctionRange(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRange>();
}

}
