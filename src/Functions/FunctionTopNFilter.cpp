#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Processors/TopNThresholdTracker.h>
#include <Interpreters/convertFieldToType.h>
#include <Functions/IFunctionAdaptors.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionTopNFilter: public IFunction
{
public:
    static constexpr auto name = "__topNFilter";

    explicit FunctionTopNFilter(TopNThresholdTrackerPtr threshold_tracker_)
        : threshold_tracker(threshold_tracker_)
    {
        direction = threshold_tracker_->getDirection();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isDeterministic() const override { return false; } /// disable query condition cache
    bool isDeterministicInScopeOfQuery() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Number of arguments for function {} can't be {}, should be 1",
                            getName(), arguments.size());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0) /// dry run?
            return ColumnUInt8::create();
        if (threshold_tracker && threshold_tracker->isSet())
        {
            auto current_threshold = threshold_tracker->getValue();
            auto data_type = arguments[0].type;

            auto threshold_column = data_type->createColumn();
            threshold_column->insert(current_threshold);

            PaddedPODArray<Int8> compare_results;
            compare_results.resize(input_rows_count);

            auto filter = ColumnVector<UInt8>::create();
            auto & filter_data = filter->getData();
            filter_data.resize(input_rows_count);

            /// The next lines are the key! Will the (compareColumn() + for loop
            /// to set filter_data[i]) be cheaper than (other predicates + sorting) ?
            /// e.g URL like '%google%' looks costly and optimization should help in
            /// SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10.
            /// Can we avoid the for loop?
            arguments[0].column->compareColumn(*threshold_column, 0, nullptr, compare_results, direction, 1);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                filter_data[i] = (compare_results[i] < 0); /// will handle both ASC and DESC
            }
            return filter;
        }
        else
        {
            return DataTypeUInt8().createColumnConst(input_rows_count, true);
        }
    }
private:
    TopNThresholdTrackerPtr threshold_tracker;
    int direction;
};

FunctionOverloadResolverPtr createInternalFunctionTopNFilterResolver(TopNThresholdTrackerPtr threshold_tracker_)
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTopNFilter>(threshold_tracker_));
};

}
