#include <Functions/FunctionTopKFilter.h>
#include <Columns/Collator.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/TypeTree.h>
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

namespace
{

/// `Tuple` comparison functions reject an empty `Tuple` nested inside another `Tuple`,
/// while the column comparison path supports it. Other composite types have their
/// own comparison implementations, so only descend through `Tuple` and `Nullable`.
bool hasEmptyTuple(const DataTypePtr & type)
{
    const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get());
    const auto & nested_type = nullable_type ? nullable_type->getNestedType() : type;
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type.get());
    if (!tuple_type)
        return false;

    if (tuple_type->getElements().empty())
        return true;

    for (const auto & element_type : tuple_type->getElements())
        if (hasEmptyTuple(element_type))
            return true;

    return false;
}

/// `lessOrEquals` and `greaterOrEquals` are false for NaN, while the sort places NaN first or last by
/// `nulls_direction`, so the comparison functions would drop NaN rows that rank before the threshold.
/// The column comparison path places NaN the way the sort does. Container types (`Tuple`, `Array`,
/// `Map`) compare their elements lexicographically, so a float anywhere inside them matters too.
bool hasFloatingPoint(const DataTypePtr & type)
{
    return anyInTypeTree(*type, [](const IDataType & node) { return isFloat(node); });
}

/// A `NULL` nested inside a container type (e.g. `Tuple(Nullable(UInt32))`) makes the comparison functions
/// return `Nullable(UInt8)` and ignores `nulls_direction`; the column comparison path handles both.
bool hasNestedNullable(const DataTypePtr & type)
{
    return anyInTypeTree(*type, [](const IDataType & node) { return node.isNullable(); });
}

}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class FunctionTopKFilter final : public IFunction
{
public:
    static constexpr auto name = "__topKFilter";

    FunctionTopKFilter(TopKThresholdTrackerPtr threshold_tracker_, std::shared_ptr<std::atomic<bool>> rows_equal_to_threshold_dropped_)
        : threshold_tracker(threshold_tracker_)
        , rows_equal_to_threshold_dropped(std::move(rows_equal_to_threshold_dropped_))
    {
        if (!threshold_tracker_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionTopKFilter got NULL threshold_tracker");

        threshold_type = recursiveRemoveLowCardinality(threshold_tracker_->getDataType());
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
            if (rows_equal_to_threshold_dropped)
            {
                better_function = FunctionFactory::instance().get(direction == -1 ? "greater" : "less", context);
                equals_function = FunctionFactory::instance().get("equals", context);
            }
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

    String getSignatureString() const override
    {
        return "(Any) -> UInt8";
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeUInt8>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// The default implementation would convert a sparse argument to a full column on every call.
    bool useDefaultImplementationForSparseColumns() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt8::create();

        if (!threshold_tracker || !threshold_tracker->isSet())
            return DataTypeUInt8().createColumnConst(input_rows_count, true);

        /// Read before the value, so that a concurrent update can only make `last_checked_version` stale.
        const UInt64 version = threshold_tracker->getVersion();
        auto current_threshold = threshold_tracker->getValue();
        const auto & argument = arguments[0];

        /// Rows equal to the threshold can be dropped only while the threshold is backed by rows of the sort this read feeds.
        if (!rows_equal_to_threshold_dropped || threshold_tracker->isSharedByPlanCopies())
            return compare(argument, current_threshold, input_rows_count, Mode::NotWorse);

        if (rows_equal_to_threshold_dropped->load(std::memory_order_relaxed))
            return compare(argument, current_threshold, input_rows_count, Mode::Better);

        auto not_worse = compare(argument, current_threshold, input_rows_count, Mode::NotWorse);
        const auto & not_worse_data = assert_cast<const ColumnUInt8 &>(*not_worse).getData();
        if (version == last_checked_version.load(std::memory_order_relaxed) || 2 * countBytesInFilter(not_worse_data) <= input_rows_count)
            return not_worse;

        auto equal = compare(argument, current_threshold, input_rows_count, Mode::Equal);
        const auto & equal_data = assert_cast<const ColumnUInt8 &>(*equal).getData();
        last_checked_version.store(version, std::memory_order_relaxed);
        if (2 * countBytesInFilter(equal_data) <= input_rows_count)
            return not_worse;

        rows_equal_to_threshold_dropped->store(true, std::memory_order_relaxed);

        auto result = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            result_data[i] = not_worse_data[i] & !equal_data[i];
        return result;
    }

private:
    enum class Mode : uint8_t
    {
        NotWorse,
        Better,
        Equal,
    };

    ColumnPtr compare(const ColumnWithTypeAndName & argument, const Field & current_threshold, size_t input_rows_count, Mode mode) const
    {
        /// The stored values of a sparse column start with its default value.
        if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(argument.column.get()))
        {
            const auto & values = column_sparse->getValuesPtr();
            auto values_result = compareValues({values, argument.type, argument.name}, current_threshold, values->size(), mode)
                                     ->convertToFullColumnIfConst();
            const auto & values_filter = assert_cast<const ColumnUInt8 &>(*values_result).getData();

            auto result = ColumnUInt8::create(input_rows_count, values_filter[0]);
            auto & result_data = result->getData();
            const auto & offsets = column_sparse->getOffsetsData();
            for (size_t i = 0; i < offsets.size(); ++i)
                result_data[offsets[i]] = values_filter[i + 1];
            return result;
        }

        return compareValues(argument, current_threshold, input_rows_count, mode)->convertToFullColumnIfConst();
    }

    ColumnPtr compareValues(
        const ColumnWithTypeAndName & argument, const Field & current_threshold, size_t input_rows_count, Mode mode) const
    {
        const auto & data_type = argument.type;
        if (collator || data_type->isNullable() || isDynamic(data_type) || isVariant(data_type) || hasEmptyTuple(data_type)
            || hasFloatingPoint(data_type) || hasNestedNullable(data_type))
            return executeGeneral(argument, current_threshold, data_type, input_rows_count, mode);

        return executeVectorized(argument, current_threshold, data_type, input_rows_count, mode);
    }

    /// Fast path: vectorized less/greater for non-nullable, non-collation types without NaN.
    ColumnPtr executeVectorized(
        const ColumnWithTypeAndName & argument,
        const Field & current_threshold,
        const DataTypePtr & data_type,
        size_t input_rows_count,
        Mode mode) const
    {
        const auto & function = mode == Mode::NotWorse ? compare_function : (mode == Mode::Better ? better_function : equals_function);
        /// The threshold is a value of the tracker's type, which a `Field` does not carry (an `Interval` is a plain `Int64`).
        ColumnPtr threshold_column = data_type->createColumnConst(
            input_rows_count, convertFieldToType(current_threshold, *data_type, threshold_type.get()));
        ColumnsWithTypeAndName args{argument, {threshold_column, data_type, {}}};
        auto elem_compare = function->build(args);
        /// getReturnTypeImpl always declares UInt8, so a comparison resolving to anything else
        /// (a Nullable nested in a Tuple makes it Nullable(UInt8)) must not be returned here.
        if (!isUInt8(elem_compare->getResultType()))
            return executeGeneral(argument, current_threshold, data_type, input_rows_count, mode);
        return elem_compare->execute(args, elem_compare->getResultType(), input_rows_count, false);
    }

    /// General path for `Nullable` (also nested), collation-aware, floating-point, and non-vectorizable `Tuple` types.
    ColumnPtr executeGeneral(
        const ColumnWithTypeAndName & argument,
        const Field & current_threshold,
        const DataTypePtr & data_type,
        size_t input_rows_count,
        Mode mode) const
    {
        const auto & col = *argument.column;

        auto threshold_field = convertFieldToType(current_threshold, *data_type, threshold_type.get());
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
        switch (mode)
        {
            case Mode::NotWorse:
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = compare_results[i] <= 0;
                break;
            case Mode::Better:
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = compare_results[i] < 0;
                break;
            case Mode::Equal:
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = compare_results[i] == 0;
                break;
        }

        return result_col;
    }

    TopKThresholdTrackerPtr threshold_tracker;
    /// The type of the threshold value as this function receives it (the default implementation removes LowCardinality).
    DataTypePtr threshold_type;
    FunctionOverloadResolverPtr compare_function;
    FunctionOverloadResolverPtr better_function;
    FunctionOverloadResolverPtr equals_function;

    std::shared_ptr<std::atomic<bool>> rows_equal_to_threshold_dropped;
    /// The tracker version of the last threshold whose ties were counted.
    mutable std::atomic<UInt64> last_checked_version{std::numeric_limits<UInt64>::max()};

    int direction;
    int nulls_direction;
    std::shared_ptr<Collator> collator;
};

FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(
    TopKThresholdTrackerPtr threshold_tracker_, std::shared_ptr<std::atomic<bool>> rows_equal_to_threshold_dropped)
{
    return std::make_shared<FunctionToOverloadResolverAdaptor>(
        std::make_shared<FunctionTopKFilter>(threshold_tracker_, std::move(rows_equal_to_threshold_dropped)));
}

}
