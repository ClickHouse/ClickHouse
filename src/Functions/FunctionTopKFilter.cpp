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

        String comparator = "less";

        if (threshold_tracker->getDirection() == -1) /// DESC
            comparator = "greater";
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
        if (input_rows_count == 0) /// dry run?
            return ColumnUInt8::create();

        if (threshold_tracker && threshold_tracker->isSet())
        {
            auto current_threshold = threshold_tracker->getValue();

            auto data_type = arguments[0].type;
            ColumnPtr threshold_column = data_type->createColumnConst(input_rows_count, convertFieldToType(current_threshold, *data_type));

            ColumnsWithTypeAndName args{ arguments[0], {threshold_column, data_type, {}} };
            auto elem_compare = compare_function->build(args);
            /// Note that using "greater" / "less" function here is more optimal than using Column::compareColumn()
            /// because greater/less implementation is AVX specific optimized.
            return elem_compare->execute(args, elem_compare->getResultType(), input_rows_count, /* dry_run = */ false);
        }
        else
        {
            return DataTypeUInt8().createColumnConst(input_rows_count, true);
        }
    }
private:
    TopKThresholdTrackerPtr threshold_tracker;
    FunctionOverloadResolverPtr compare_function;

    int direction;
};

FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_)
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTopKFilter>(threshold_tracker_));
};

}
