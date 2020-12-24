#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_H3

#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#include <h3api.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int PARAMETER_OUT_OF_BOUND;
}

namespace
{

class FunctionH3KRing : public IFunction
{
public:
    static constexpr auto name = "h3kRing";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionH3KRing>(); }

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
        if (!isInteger(arg))
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(2) + " of function " + getName() + ". Must be integer",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();
        const auto * col_k = arguments[1].column.get();

        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        auto current_offset = 0;

        std::vector<H3Index> hindex_vec;

        for (const auto row : ext::range(0, input_rows_count))
        {
            const H3Index origin_hindex = col_hindex->getUInt(row);
            const int k = col_k->getInt(row);

            /// Overflow is possible. The function maxKringSize does not check for overflow.
            /// The calculation is similar to square of k but several times more.
            /// Let's use huge underestimation as the safe bound. We should not allow to generate too large arrays nevertheless.
            constexpr auto max_k = 10000;
            if (k > max_k)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Too large 'k' argument for {} function, maximum {}", getName(), max_k);
            if (k < 0)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Argument 'k' for {} function must be non negative", getName());

            const auto vec_size = maxKringSize(k);
            hindex_vec.resize(vec_size);
            kRing(origin_hindex, k, hindex_vec.data());

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

void registerFunctionH3KRing(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3KRing>();
}

}

#endif
