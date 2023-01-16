#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <pcg_random.hpp>
#include <Common/assert_cast.h>
#include <Common/randomSeed.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Shuffle array elements
 * arrayShuffle(arr)
 * arrayShuffle(arr, seed)
 */
class FunctionArrayShuffle : public IFunction
{
public:
    static constexpr auto name = "arrayShuffle";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayShuffle>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 2 || arguments.empty())
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs 1..2 arguments; passed {}.", getName(), arguments.size());
        }

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            WhichDataType which(arguments[1]);
            if (!which.isUInt() && !which.isInt())
                throw Exception(
                    "Illegal type " + arguments[1]->getName() + " of argument of function " + getName() + " (must be UInt or Int)",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override;

private:
    static ColumnPtr executeGeneric(const ColumnArray & array, ColumnPtr mapped, pcg64_fast & rng);
};

ColumnPtr FunctionArrayShuffle::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!array)
        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

    const auto seed = [&]() -> uint64_t
    {
        if (arguments.size() == 1)
            return randomSeed();
        const auto * val = arguments[1].column.get();
        return val->getUInt(0);
    }();
    pcg64_fast rng(seed);

    return executeGeneric(*array, array->getDataPtr(), rng);
}

ColumnPtr FunctionArrayShuffle::executeGeneric(const ColumnArray & array, ColumnPtr /*mapped*/, pcg64_fast & rng)
{
    const ColumnArray::Offsets & offsets = array.getOffsets();

    size_t size = offsets.size();
    size_t nested_size = array.getData().size();
    IColumn::Permutation permutation(nested_size);
    std::iota(std::begin(permutation), std::end(permutation), 0);

    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto next_offset = offsets[i];
        std::shuffle(&permutation[current_offset], &permutation[next_offset], rng);
        current_offset = next_offset;
    }
    return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
}

REGISTER_FUNCTION(ArrayShuffle)
{
    factory.registerFunction<FunctionArrayShuffle>(
        {
            R"(
Returns an array of the same size as the original array containing the elements in shuffled order.
Elements are being reordered in such a way that each possible permutation of those elements has equal probability of appearance.

If no seed is provided a random one will be used:
[example:random_seed]

It is possible to override the seed to produce stable results:
[example:explicit_seed]
)",
         Documentation::Examples{
                {"random_seed", "SELECT arrayShuffle([1, 2, 3, 4])"},
                {"explicit_seed", "SELECT arrayShuffle([1, 2, 3, 4], 41)"}},
            Documentation::Categories{"Array"}
        },
        FunctionFactory::CaseInsensitive);
}

}
