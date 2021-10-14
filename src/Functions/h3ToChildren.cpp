#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <ext/range.h>

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
}

namespace
{

class FunctionH3ToChildren : public IFunction
{
public:
    static constexpr auto name = "h3ToChildren";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionH3ToChildren>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be UInt64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt8())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(2) + " of function " + getName() + ". Must be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();
        const auto * col_resolution = arguments[1].column.get();

        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        auto current_offset = 0;

        std::vector<H3Index> hindex_vec;

        for (const auto row : ext::range(0, input_rows_count))
        {
            const UInt64 parent_hindex = col_hindex->getUInt(row);
            const UInt8 child_resolution = col_resolution->getUInt(row);

            if (child_resolution > MAX_H3_RES)
                throw Exception("The argument 'resolution' (" + toString(child_resolution) + ") of function " + getName()
                    + " is out of bounds because the maximum resolution in H3 library is " + toString(MAX_H3_RES), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            const size_t vec_size = maxH3ToChildrenSize(parent_hindex, child_resolution);
            if (vec_size > MAX_ARRAY_SIZE)
                throw Exception("The result of function" + getName()
                    + " (array of " + toString(vec_size) + " elements) will be too large with resolution argument = "
                    + toString(child_resolution), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

            hindex_vec.resize(vec_size);
            h3ToChildren(parent_hindex, child_resolution, hindex_vec.data());

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
