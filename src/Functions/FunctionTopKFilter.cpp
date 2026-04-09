#include <Columns/Collator.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionTopKFilter : public IFunction
{
public:
    static constexpr auto name = "__topKFilter";

    explicit FunctionTopKFilter(TopKThresholdTrackerPtr threshold_tracker_)
        : threshold_tracker(threshold_tracker_)
    {
        if (!threshold_tracker_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionTopKFilter got NULL threshold_tracker");

        direction = threshold_tracker_->getDirection();
        nulls_direction = threshold_tracker_->getNullsDirection();
        collator = threshold_tracker_->getCollator();

        if (!collator)
        {
            String comparator = "lessOrEquals";
            if (direction == -1)
                comparator = "greaterOrEquals";
            auto context = Context::getGlobalContextInstance();
            compare_function = FunctionFactory::instance().get(comparator, context);
        }
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} can't be {}, should be 1",
                getName(),
                arguments.size());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeUInt8>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt8::create();

        if (threshold_tracker && threshold_tracker->isSet())
        {
            auto current_threshold = threshold_tracker->getValue();
            auto data_type = arguments[0].type;

            if (collator || data_type->isNullable())
                return executeGeneral(arguments[0], current_threshold, data_type, input_rows_count);

            return executeVectorized(arguments[0], current_threshold, data_type, input_rows_count);
        }
        else
        {
            return DataTypeUInt8().createColumnConst(input_rows_count, true);
        }
    }

private:
    /// Fast path: vectorized less/greater for non-nullable, non-collation types.
    ColumnPtr executeVectorized(
        const ColumnWithTypeAndName & argument,
        const Field & current_threshold,
        const DataTypePtr & data_type,
        size_t input_rows_count) const
    {
        ColumnPtr threshold_column = data_type->createColumnConst(input_rows_count, convertFieldToType(current_threshold, *data_type));
        ColumnsWithTypeAndName args{argument, {threshold_column, data_type, {}}};
        auto elem_compare = compare_function->build(args);
        return elem_compare->execute(args, elem_compare->getResultType(), input_rows_count, false);
    }

    /// General path for Nullable and/or collation-aware types.
    ColumnPtr executeGeneral(
        const ColumnWithTypeAndName & argument,
        const Field & current_threshold,
        const DataTypePtr & data_type,
        size_t input_rows_count) const
    {
        const auto & col = *argument.column;

        auto threshold_field = convertFieldToType(current_threshold, *data_type);
        auto threshold_col = data_type->createColumn();
        threshold_col->insert(threshold_field);

        PaddedPODArray<Int8> compare_results;

        if (collator)
        {
            compare_results.resize(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                int cmp = col.compareAtWithCollation(i, 0, *threshold_col, nulls_direction, *collator);
                compare_results[i] = static_cast<Int8>(direction * cmp);
            }
        }
        else
        {
            col.compareColumn(*threshold_col, 0, nullptr, compare_results, direction, nulls_direction);
        }

        auto result_col = ColumnUInt8::create(input_rows_count);
        auto & result_data = result_col->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            result_data[i] = compare_results[i] <= 0;

        return result_col;
    }

    TopKThresholdTrackerPtr threshold_tracker;
    FunctionOverloadResolverPtr compare_function;

    int direction;
    int nulls_direction;
    std::shared_ptr<Collator> collator;
};

FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_)
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTopKFilter>(threshold_tracker_));
}

}
