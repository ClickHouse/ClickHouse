#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Processors/TopKThresholdTracker.h>
#include <Interpreters/convertFieldToType.h>
#include <Functions/IFunctionAdaptors.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionTopKFilter: public IFunction
{
public:
    static constexpr auto name = "__topKFilter";

    explicit FunctionTopKFilter(TopKThresholdTrackerPtr threshold_tracker_)
        : threshold_tracker(threshold_tracker_)
    {
        if (!threshold_tracker_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionTopKFilter got NULL threshold_tracker");

        String comparator = "lessOrEquals";

        if (threshold_tracker->getDirection() == -1) /// DESC
            comparator = "greaterOrEquals";
        auto context = Context::getGlobalContextInstance();
        compare_function = FunctionFactory::instance().get(comparator, context);
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
        if (input_rows_count == 0)
            return ColumnUInt8::create();

        if (!threshold_tracker || !threshold_tracker->isSet())
            return DataTypeUInt8().createColumnConst(input_rows_count, true);

        uint64_t current_version = threshold_tracker->getVersion();
        FunctionBasePtr comparator;
        Field converted_threshold;
        {
            std::lock_guard lock(cache_mutex);
            if (current_version != cached_version || !cached_comparator)
            {
                cached_version = current_version;
                cached_threshold = threshold_tracker->getValue();
                auto data_type = arguments[0].type;
                cached_converted_threshold = convertFieldToType(cached_threshold, *data_type);
                auto threshold_col = data_type->createColumnConst(1, cached_converted_threshold);
                ColumnsWithTypeAndName build_args{arguments[0], {threshold_col, data_type, {}}};
                cached_comparator = compare_function->build(build_args);
            }
            comparator = cached_comparator;
            converted_threshold = cached_converted_threshold;
        }

        auto data_type = arguments[0].type;
        auto threshold_col = data_type->createColumnConst(input_rows_count, converted_threshold);
        ColumnsWithTypeAndName exec_args{arguments[0], {threshold_col, data_type, {}}};
        return comparator->execute(exec_args, comparator->getResultType(), input_rows_count, false);
    }
private:
    TopKThresholdTrackerPtr threshold_tracker;
    FunctionOverloadResolverPtr compare_function;

    int direction;

    mutable std::mutex cache_mutex;
    mutable uint64_t cached_version{0};
    mutable Field cached_threshold;
    mutable Field cached_converted_threshold;
    mutable FunctionBasePtr cached_comparator;
};

FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_)
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTopKFilter>(threshold_tracker_));
};

}
