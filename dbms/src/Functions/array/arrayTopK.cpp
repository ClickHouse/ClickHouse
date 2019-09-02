#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include "FunctionArrayMapped.h"
#include <Core/iostream_debug_helpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Accepting an array and a number K, returns an array with top K elements marked as one, while others as zero.
  */

template <bool positive>
class FunctionArrayTopKBase : public IFunction
{
public:
    FunctionArrayTopKBase(const Context & context_, const char * name_) : context(context_), name(name_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                    + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->onlyNull())
            return arguments[0];

        auto array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception(
                "First argument for function " + getName() + " must be an array but it has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        for (size_t i = 1; i < number_of_arguments; ++i)
        {
            if (!isUnsignedInteger(removeNullable(arguments[i])) && !arguments[i]->onlyNull())
                throw Exception(
                    "Argument " + toString(i) + " for function " + getName() + " must be unsigned integer but it has type "
                        + arguments[i]->getName() + ".",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    struct Greater
    {
        const IColumn & column;

        Greater(const IColumn & column_) : column(column_) {}

        bool operator()(size_t lhs, size_t rhs) const
        {
            if constexpr (positive)
                return column.compareAt(lhs, rhs, column, -1) > 0;
            else
                return column.compareAt(lhs, rhs, column, 1) < 0;
        }
    };

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        ColumnPtr column_array_ptr = block.getByPosition(arguments[0]).column;
        const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        const auto & topk_column = block.getByPosition(arguments[1]).column;

        if (!column_array)
        {
            const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
            if (!column_const_array)
                throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
            column_array_ptr = column_const_array->convertToFullColumn();
            column_array = assert_cast<const ColumnArray *>(column_array_ptr.get());
        }

        auto & array = *column_array;
        auto mapped = column_array->getDataPtr();

        const ColumnArray::Offsets & offsets = array.getOffsets();

        size_t size = offsets.size();
        size_t nested_size = array.getData().size();
        IColumn::Permutation permutation(nested_size);

        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        auto mask_column = ColumnUInt64::create(nested_size);
        auto & mask = mask_column->getData();
        if (isColumnConst(*topk_column))
        {
            size_t topk = topk_column->getUInt(0);
            if (topk == 0)
            {
                for (auto j = 0ul; j < nested_size; ++j)
                    mask[j] = 0;
            }
            else
            {
                ColumnArray::Offset current_offset = 0;
                for (size_t i = 0; i < size; ++i)
                {
                    auto next_offset = offsets[i];
                    if (next_offset - current_offset <= topk)
                    {
                        for (auto j = current_offset; j < next_offset; ++j)
                            mask[j] = 1;
                    }
                    else
                    {
                        std::sort(&permutation[current_offset], &permutation[next_offset], Greater(*mapped));
                        auto j = current_offset, k = 0ul;
                        for (; j < next_offset && k < topk; ++j, ++k)
                            mask[permutation[j]] = 1;
                        for (; j < next_offset; ++j)
                            mask[permutation[j]] = 0;
                    }
                    current_offset = next_offset;
                }
            }
        }
        else
        {
            ColumnArray::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                size_t topk = topk_column->getUInt(i);
                auto next_offset = offsets[i];
                if (next_offset - current_offset <= topk)
                {
                    for (auto j = current_offset; j < next_offset; ++j)
                        mask[j] = 1;
                }
                else if (topk == 0)
                {
                    for (auto j = current_offset; j < next_offset; ++j)
                        mask[j] = 0;
                }

                else
                {
                    std::sort(&permutation[current_offset], &permutation[next_offset], Greater(*mapped));
                    auto j = current_offset, k = 0ul;
                    for (; j < next_offset && k < topk; ++j, ++k)
                        mask[permutation[j]] = 1;
                    for (; j < next_offset; ++j)
                        mask[permutation[j]] = 0;
                }
                current_offset = next_offset;
            }
        }

        DUMP(block.getByPosition(result));
        block.getByPosition(result).column = ColumnArray::create(std::move(mask_column), array.getOffsetsPtr());
    }

private:
    const Context & context;
    const char * name;
};

class FunctionArrayTopK : public FunctionArrayTopKBase<true>
{
public:
    static constexpr auto name = "arrayTopK";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayTopK>(context); }
    FunctionArrayTopK(const Context & context_) : FunctionArrayTopKBase(context_, name) {}
};

class FunctionArrayReverseTopK : public FunctionArrayTopKBase<false>
{
public:
    static constexpr auto name = "arrayReverseTopK";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayReverseTopK>(context); }
    FunctionArrayReverseTopK(const Context & context_) : FunctionArrayTopKBase(context_, name) {}
};

void registerFunctionsArrayTopK(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayTopK>();
    factory.registerFunction<FunctionArrayReverseTopK>();
}

}
