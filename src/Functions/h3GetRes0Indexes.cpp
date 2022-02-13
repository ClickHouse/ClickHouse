#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <h3api.h>


namespace DB
{
namespace
{

class FunctionH3GetRes0Indexes final : public IFunction
{
public:
    static constexpr auto name = "h3GetRes0Indexes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetRes0Indexes>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);
        auto current_offset = 0;

        const auto vec_size = res0CellCount();
        std::vector<H3Index> hindex_vec;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            hindex_vec.resize(vec_size);
            getRes0Cells(hindex_vec.data());
            dst_data.reserve(input_rows_count);

            for (auto & hindex : hindex_vec)
            {
                ++current_offset;
                dst_data.insert(hindex);
            }
            hindex_vec.clear();
            dst_offsets[row] = current_offset;
        }
        return dst;
    }
};

}

void registerFunctionH3GetRes0Indexes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3GetRes0Indexes>();
}

}

#endif
