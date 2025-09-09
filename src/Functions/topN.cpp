#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Processors/Transforms/PartialSortingTransform.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace
{

class FunctionTopN : public IFunction
{
public:
    static constexpr auto name = "__topN";

    explicit FunctionTopN(GlobalThresholdColumnsPtr global_threshold_columns_ptr_)
        : global_threshold_columns_ptr(std::move(global_threshold_columns_ptr_))
    {
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool useDefaultImplementationForNothing() const override { return true; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto current_ptr = global_threshold_columns_ptr->getCurrentThreshold();
        if (current_ptr->sorting_transform == nullptr)
            return ColumnUInt8::create(input_rows_count, true);

        auto ret = ColumnUInt8::create(input_rows_count);
        PaddedPODArray<Int8> compare_results;
        current_ptr->sorting_transform->getFilterMask(
            {arguments[0].column.get()},
            current_ptr->columns,
            input_rows_count,
            ret->getData(),
            nullptr,
            compare_results,
            current_ptr->columns.size() > 1);
        return ret;
    }

private:
    GlobalThresholdColumnsPtr global_threshold_columns_ptr;
};

}

FunctionOverloadResolverPtr createInternalFunctionTopN(GlobalThresholdColumnsPtr global_threshold_columns_ptr)
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTopN>(std::move(global_threshold_columns_ptr)));
};

}
