#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>

#include <iostream>

namespace DB {
namespace ErrorCodes {
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/*
* arrayWithConstant(num, const) - make array of constants with length num.
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
        if (!isUnsignedInteger(arguments[0])) {
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected Integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return std::make_shared<DataTypeArray>(arguments[1]);
    }


    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const auto * num = block.getByPosition(arguments[0]).column.get();
        const auto * constant = block.getByPosition(arguments[1]).column.get();
        uint64_t a = num->getUInt(0);

        auto offsets_col = ColumnArray::ColumnOffsets::create();
        ColumnArray::Offsets & offsets = offsets_col->getData();

        offsets.push_back(a);
        auto data_col = constant->replicate(offsets);
        block.getByPosition(result).column = ColumnArray::create(std::move(data_col), std::move(offsets_col));
    }
};

void registerFunctionArrayWithConstant(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayWithConstant>();
}

} // namespace DB




