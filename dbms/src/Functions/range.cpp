#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
}


class FunctionRange : public IFunction
{
public:
    static constexpr auto name = "range";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRange>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & arg = arguments.front();

        if (!isUnsignedInteger(arg))
            throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeArray>(arg);
    }

    template <typename T>
    bool executeInternal(Block & block, const IColumn * arg, const size_t result)
    {
        static constexpr size_t max_elements = 100'000'000;

        if (const auto in = checkAndGetColumn<ColumnVector<T>>(arg))
        {
            const auto & in_data = in->getData();
            const auto total_values = std::accumulate(std::begin(in_data), std::end(in_data), size_t{},
                [this] (const size_t lhs, const size_t rhs)
                {
                    const auto sum = lhs + rhs;
                    if (sum < lhs)
                        throw Exception{"A call to function " + getName() + " overflows, investigate the values of arguments you are passing",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

                    return sum;
                });

            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};

            auto data_col = ColumnVector<T>::create(total_values);
            auto offsets_col = ColumnArray::ColumnOffsets::create(in->size());

            auto & out_data = data_col->getData();
            auto & out_offsets = offsets_col->getData();

            IColumn::Offset offset{};
            for (size_t row_idx = 0, rows = in->size(); row_idx < rows; ++row_idx)
            {
                for (size_t elem_idx = 0, elems = in_data[row_idx]; elem_idx < elems; ++elem_idx)
                    out_data[offset + elem_idx] = elem_idx;

                offset += in_data[row_idx];
                out_offsets[row_idx] = offset;
            }

            block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
            return true;
        }
        else
            return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const auto col = block.getByPosition(arguments[0]).column.get();

        if (!executeInternal<UInt8>(block, col, result) &&
            !executeInternal<UInt16>(block, col, result) &&
            !executeInternal<UInt32>(block, col, result) &&
            !executeInternal<UInt64>(block, col, result))
        {
            throw Exception{"Illegal column " + col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }
};


void registerFunctionRange(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRange>();
}

}
