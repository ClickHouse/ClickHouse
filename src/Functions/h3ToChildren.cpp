#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3ToChildren : public IFunction
{
public:
    static constexpr auto name = "h3ToChildren";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToChildren>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt8",
                arg->getName(), 2, getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_hindex = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_hindex)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data_hindex = col_hindex->getData();

        const auto * col_resolution = checkAndGetColumn<ColumnUInt8>(non_const_arguments[1].column.get());
        if (!col_resolution)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt8.",
                arguments[1].type->getName(),
                2,
                getName());

        const auto & data_resolution = col_resolution->getData();


        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        auto current_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 parent_hindex = data_hindex[row];
            const UInt8 child_resolution = data_resolution[row];

            if (child_resolution > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                    toString(child_resolution), getName(), toString(MAX_H3_RES));

            const size_t vec_size = cellToChildrenSize(parent_hindex, child_resolution);
            if (vec_size > MAX_ARRAY_SIZE)
                throw Exception(
                    ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "The result of function {} (array of {} elements) will be too large with resolution argument = {}",
                    getName(), toString(vec_size), toString(child_resolution));

            std::vector<H3Index> hindex_vec;
            hindex_vec.resize(vec_size);
            cellToChildren(parent_hindex, child_resolution, hindex_vec.data());

            dst_data.reserve(dst_data.size() + vec_size);
            for (auto hindex : hindex_vec)
            {
                if (hindex != 0)
                {
                    ++current_offset;
                    dst_data.insert(hindex);
                }
            }
            dst_offsets[row] = current_offset;
        }

        return dst;
    }
};

}

void registerFunctionH3ToChildren(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3ToChildren>();
}

}

#endif
