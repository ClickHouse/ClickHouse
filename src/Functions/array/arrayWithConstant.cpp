#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/// Reasonable threshold.
static constexpr size_t max_arrays_size_in_block = 1000000000;


/* arrayWithConstant(num, const) - make array of constants with length num.
 * arrayWithConstant(3, 'hello') = ['hello', 'hello', 'hello']
 * arrayWithConstant(1, 'hello') = ['hello']
 * arrayWithConstant(0, 'hello') = []
 */

class FunctionArrayWithConstant : public IFunction
{
public:
    static constexpr auto name = "arrayWithConstant";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayWithConstant>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNativeNumber(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() +
                " of argument of function " + getName() +
                ", expected Integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeArray>(arguments[1]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t num_rows) const override
    {
        const auto * col_num = block.getByPosition(arguments[0]).column.get();
        const auto * col_value = block.getByPosition(arguments[1]).column.get();

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnArray::Offsets & offsets = offsets_col->getData();
        offsets.reserve(num_rows);

        ColumnArray::Offset offset = 0;
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto array_size = col_num->getInt(i);

            if (unlikely(array_size < 0))
                throw Exception("Array size cannot be negative: while executing function " + getName(), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

            offset += array_size;

            if (unlikely(offset > max_arrays_size_in_block))
                throw Exception("Too large array size while executing function " + getName(), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

            offsets.push_back(offset);
        }

        block.getByPosition(result).column = ColumnArray::create(col_value->replicate(offsets)->convertToFullColumnIfConst(), std::move(offsets_col));
    }
};

void registerFunctionArrayWithConstant(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayWithConstant>();
}

}
