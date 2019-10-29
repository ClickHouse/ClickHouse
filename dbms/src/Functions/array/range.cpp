#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <numeric>

#include <common/logger_useful.h>


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
        if (arguments.size() > 2 || arguments.empty())
        {
            throw Exception{"Function " + getName() + " needs [1-2] argument; passed "
                            + std::to_string(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
        }

        for (const auto & arg : arguments)
        {
            if (!isUnsignedInteger(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        return std::make_shared<DataTypeArray>(arguments.back());
    }

    template <typename T, typename E>
    bool executeInternal(Block & block, const IColumn * start_col, const IColumn * end_col, const size_t result)
    {
        static constexpr size_t max_elements = 100'000'000;
        
        auto start_column = checkAndGetColumn<ColumnVector<T>>(start_col);
        auto end_column = checkAndGetColumn<ColumnVector<E>>(end_col);

        if (!start_column || !end_column)
        {
            LOG_TRACE(&Logger::get("range function"),  "some column is null-----"); 
            return false;
        }

        const auto & in_start_data = start_column->getData();
        const auto & in_end_data = end_column->getData();

        size_t total_values = 0;

        for (size_t row_idx = 0, rows = end_column->size(); row_idx < rows; ++row_idx)
        {
            total_values += in_start_data[row_idx] >= in_end_data[row_idx] ? 0 : (in_end_data[row_idx] -in_start_data[row_idx]);
            if (total_values > max_elements)
                throw Exception{"A call to function " + getName() + " would produce " + std::to_string(total_values) +
                    " array elements, which is greater than the allowed maximum of " + std::to_string(max_elements),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND};
        }

        auto data_col = ColumnVector<E>::create(total_values);
        auto offsets_col = ColumnArray::ColumnOffsets::create(end_column->size());

        auto & out_data = data_col->getData();
        auto & out_offsets = offsets_col->getData();

        for (const auto & data : in_start_data)
        {
            LOG_TRACE(&Logger::get("range function"),  "Test--" << UInt32(data) << std::endl);
        }

        IColumn::Offset offset{};
        for (size_t row_idx = 0, rows = end_column->size(); row_idx < rows; ++row_idx)
        {
            LOG_TRACE(&Logger::get("range function"),  "DEBUG--" << UInt64(start_col->getUInt(row_idx)) << "---" << UInt64(in_end_data[row_idx]) << "--" << UInt64(offset)); 
            for (size_t st = in_start_data[row_idx], ed = in_end_data[row_idx]; st < ed; ++st)
                out_data[offset++] = st;

            out_offsets[row_idx] = offset;
        }

        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
        return true;
    }
    
    template <typename T>
    bool executeStartInternal(Block & block, const IColumn * start_col, const IColumn * end_col, const size_t result)
    {
        return executeInternal<T, UInt8>(block, start_col, end_col, result)
               || executeInternal<T, UInt16>(block, start_col, end_col, result)
               || executeInternal<T, UInt32>(block, start_col, end_col, result)
               || executeInternal<T, UInt64>(block, start_col, end_col, result);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {   
        const auto end_col = block.getByPosition(arguments[arguments.size() == 1 ? 0 : 1]).column.get();
        const auto start_col = arguments.size() == 1? ColumnVector<UInt8>::create(end_col->size(), 0).get()
                            : block.getByPosition(arguments[0]).column.get();

        if (!executeStartInternal<UInt8>(block, start_col, end_col, result) &&
            !executeStartInternal<UInt16>(block, start_col, end_col, result) &&
            !executeStartInternal<UInt32>(block, start_col, end_col, result) &&
            !executeStartInternal<UInt64>(block, start_col, end_col, result))
        {
            throw Exception{"Illegal column " + start_col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }
    }

};


void registerFunctionRange(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRange>();
}

}
