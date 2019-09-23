#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Core/iostream_debug_helpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

class ArrayRoundCarry : public IFunction
{
public:
    static constexpr auto name = "arrayRoundCarry";

    static FunctionPtr create(const Context &) { return std::make_shared<ArrayRoundCarry>(); }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                    + ", expected Array of Array of Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr nested_type = checkAndGetDataType<DataTypeArray>(arguments[0].get())->getNestedType();

        if (!isArray(nested_type))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                    + ", expected Array of Array of Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        nested_type = checkAndGetDataType<DataTypeArray>(nested_type.get())->getNestedType();

        if (!WhichDataType(*nested_type).isFloat64())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                    + ", expected Array of Array of Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isArray(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName() + ", expected Array of Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        nested_type = checkAndGetDataType<DataTypeArray>(arguments[1].get())->getNestedType();

        if (!WhichDataType(*nested_type).isFloat64())
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName() + ", expected Array of Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    struct Greater
    {
        const Float64 * column;

        Greater(const Float64 * column_) : column(column_) { }

        bool operator()(size_t lhs, size_t rhs) const { return column[lhs] > column[rhs]; }
    };

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const ColumnArray * src_col = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!src_col)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " in argument of function 'arrayRoundCarry'",
                ErrorCodes::ILLEGAL_COLUMN);

        const ColumnArray * target_col = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[1]).column.get());

        if (!target_col)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[1]).column->getName() + " in argument of function 'arrayRoundCarry'",
                ErrorCodes::ILLEGAL_COLUMN);

        auto result_col = src_col->cloneResized(src_col->size());
        if (block.rows())
        {
            ColumnArray * res_col = typeid_cast<ColumnArray *>(&*result_col);
            const IColumn::Offsets & offsets = src_col->getOffsets();
            auto & src_data = typeid_cast<const ColumnArray &>(src_col->getData());
            auto & result_data = typeid_cast<ColumnArray &>(res_col->getData());
            auto & target_data = typeid_cast<const ColumnFloat64 &>(target_col->getData());
            auto & target = target_data.getData();

            const IColumn::Offsets & sub_offsets = src_data.getOffsets();
            auto & sub_src_data = typeid_cast<const ColumnFloat64 &>(src_data.getData());
            auto & sub_result_data = typeid_cast<ColumnFloat64 &>(result_data.getData());

            auto & sub_src = sub_src_data.getData();
            auto & sub_result = sub_result_data.getData();

            size_t pos = 0, sub_pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                size_t col_size = sub_offsets[pos] - sub_offsets[pos - 1];
                IColumn::Permutation permutation(col_size);
                PaddedPODArray<Float64> delta(col_size, 0.0l);
                auto greater = Greater(delta.data());
                for (; pos < offsets[i]; ++pos)
                {
                    Float64 fsum = 0.0l;
                    for (size_t j = 0; sub_pos < sub_offsets[pos]; ++sub_pos, ++j)
                    {
                        permutation[j] = j;
                        auto prev_val = sub_src[sub_pos] * 100 + delta[j];
                        fsum += sub_result[sub_pos] = prev_val > 0 ? (floor(prev_val / 100) * 100) : 0;
                        delta[j] = prev_val - sub_result[sub_pos];
                    }
                    std::sort(permutation.begin(), permutation.end(), greater);
                    for (auto j = 0ul; j < std::min(sub_offsets[pos] - sub_offsets[pos - 1], static_cast<UInt64>(target[pos] - fsum / 100));
                         ++j)
                    {
                        sub_result[permutation[j] + sub_offsets[pos - 1]] += 100;
                        delta[permutation[j]] -= 100;
                    }
                }
            }
            for (auto & elem : sub_result)
            {
                elem /= 100;
            }
        }

        block.getByPosition(result).column = std::move(result_col);
    }

private:
    String getName() const override { return name; }
};


void registerFunctionArrayRoundCarry(FunctionFactory & factory)
{
    factory.registerFunction<ArrayRoundCarry>();
    factory.registerAlias("rc", "arrayRoundCarry", FunctionFactory::CaseInsensitive);
}
}
