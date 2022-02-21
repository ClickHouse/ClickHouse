#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

class FunctionH3GetPentagonIndexes : public IFunction
{
public:
    static constexpr auto name = "h3GetPentagonIndexes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetPentagonIndexes>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt8>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt8.",
                arguments[0].type->getName(),
                1,
                getName());
        const auto & data = column->getData();

        auto result_column_data = ColumnUInt64::create();
        auto & result_data = result_column_data->getData();

        auto result_column_offsets = ColumnArray::ColumnOffsets::create();
        auto & result_offsets = result_column_offsets->getData();
        result_offsets.resize(input_rows_count);

        auto current_offset = 0;
        std::vector<H3Index> hindex_vec;
        result_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (data[row] > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is ",
                    toString(data[row]),
                    getName(),
                    MAX_H3_RES);

            const auto vec_size = pentagonCount();
            hindex_vec.resize(vec_size);
            getPentagons(data[row], hindex_vec.data());

            for (auto & i : hindex_vec)
            {
                ++current_offset;
                result_data.emplace_back(i);
            }

            result_offsets[row] = current_offset;
            hindex_vec.clear();
        }

        return ColumnArray::create(std::move(result_column_data), std::move(result_column_offsets));
    }
};

}

void registerFunctionH3GetPentagonIndexes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3GetPentagonIndexes>();
}

}

#endif
