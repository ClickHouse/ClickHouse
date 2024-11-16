#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** The function takes two arrays: scores and labels.
  */

class FunctionArrayPrAUC : public IFunction
{
public:
    static constexpr auto name = "arrayPrAUC";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPrAUC>(); }

private:
    static Float64
    apply(const IColumn & scores, const IColumn & labels, ColumnArray::Offset current_offset, ColumnArray::Offset next_offset)
    {
        struct ScoreLabel
        {
            Float64 score;
            bool label;
        };

        size_t size = next_offset - current_offset;
        PODArrayWithStackMemory<ScoreLabel, 1024> sorted_labels(size);

        for (size_t i = 0; i < size; ++i)
        {
            bool label = labels.getFloat64(current_offset + i) > 0;
            sorted_labels[i].score = scores.getFloat64(current_offset + i);
            sorted_labels[i].label = label;
        }

        /// Sorting scores in descending order to traverse the Precision Recall curve from left to right
        std::sort(sorted_labels.begin(), sorted_labels.end(), [](const auto & lhs, const auto & rhs) { return lhs.score > rhs.score; });

        size_t prev_tp = 0;
        size_t curr_fp = 0, curr_tp = 0;
        Float64 curr_precision = 1.0;
        Float64 area = 0.0;

        for (size_t i = 0; i < size; ++i)
        {
            if (curr_tp != prev_tp)
            {
                /* Precision = TP / (TP + FP) and Recall = TP / (TP + FN)
                 *
                 *  Instead of calculating 
                 *      d_Area = Precision_n * (Recall_n - Recall_{n-1}), 
                 *  we can calculate 
                 *      d_Area = Precision_n * (TP_n - TP_{n-1}) 
                 *  and later divide it by (TP + FN), since 
                 */
                curr_precision = static_cast<Float64>(curr_tp) / (curr_tp + curr_fp);
                area += curr_precision * (curr_tp - prev_tp);
                prev_tp = curr_tp;
            }

            if (sorted_labels[i].label)
                curr_tp += 1;
            else
                curr_fp += 1;
        }

        curr_precision = (curr_tp + curr_fp) > 0 ? static_cast<Float64>(curr_tp) / (curr_tp + curr_fp) : 1.0;
        area += curr_precision * (curr_tp - prev_tp);

        /// If there were no labels, return NaN
        if (curr_tp == 0 && curr_fp == 0)
            return std::numeric_limits<Float64>::quiet_NaN();
        /// If there were no positive labels, the only point of the curve is (0, 1) and AUC is 0
        if (curr_tp == 0)
            return 0.0;
        /// Finally, divide it by total number of positive labels (TP + FN)
        return area / curr_tp;
    }

    static void vector(
        const IColumn & scores,
        const IColumn & labels,
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<Float64> & result,
        size_t input_rows_count)
    {
        result.resize(input_rows_count);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto next_offset = offsets[i];
            result[i] = apply(scores, labels, current_offset, next_offset);
            current_offset = next_offset;
        }
    }

public:
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2.",
                getName(),
                number_of_arguments);

        for (size_t i = 0; i < 2; ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
            if (!array_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The two first arguments for function {} must be of type Array.", getName());

            const auto & nested_type = array_type->getNestedType();
            if (!isNativeNumber(nested_type) && !isEnum(nested_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} cannot process values of type {}", getName(), nested_type->getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeFloat64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr col1 = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr col2 = arguments[1].column->convertToFullColumnIfConst();

        const ColumnArray * col_array1 = checkAndGetColumn<ColumnArray>(col1.get());
        if (!col_array1)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());

        const ColumnArray * col_array2 = checkAndGetColumn<ColumnArray>(col2.get());
        if (!col_array2)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second argument of function {}",
                arguments[1].column->getName(),
                getName());

        if (!col_array1->hasEqualOffsets(*col_array2))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Array arguments for function {} must have equal sizes", getName());

        auto col_res = ColumnVector<Float64>::create();

        vector(col_array1->getData(), col_array2->getData(), col_array1->getOffsets(), col_res->getData(), input_rows_count);

        return col_res;
    }
};


REGISTER_FUNCTION(ArrayPrAUC)
{
    factory.registerFunction<FunctionArrayPrAUC>();
}

}
