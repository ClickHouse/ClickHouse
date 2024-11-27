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
  * Label can be one of two values: positive and negative.
  * Score can be arbitrary number.
  *
  * These values are considered as the output of classifier. We have some true labels for objects.
  * And classifier assigns some scores to objects that predict these labels in the following way:
  * - we can define arbitrary threshold on score and predict that the label is positive if the score is greater than the threshold:
  *
  * f(object) = score
  * predicted_label = score > threshold
  *
  * This way classifier may predict positive or negative value correctly - true positive (tp) or true negative (tn)
  *   or have false positive (fp) or false negative (fn) result.
  * Varying the threshold we can get different probabilities of false positive or false negatives or true positives, etc...
  *
  * We can also calculate the Precision and the Recall:
  *
  * Precision is the ratio `tp / (tp + fp)` where `tp` is the number of true positives and `fp` the number of false positives.
  * It represents how often the classifier is correct when giving a positive result.
  * Precision = P(label = positive | score > threshold)
  *
  * Recall is the ratio `tp / (tp + fn)` where `tp` is the number of true positives and `fn` the number of false negatives.
  * It represents the probability of the classifier to give positive result if the object has positive label.
  * Recall = P(score > threshold | label = positive)
  *
  * We can draw a curve of values of Precision and Recall with different threshold on [0..1] x [0..1] unit square.
  * This curve is named "Precision Recall curve" (PR).
  *
  * For the curve we can calculate, literally, Area Under the Curve, that will be in the range of [0..1].
  *
  * Let's look at the example:
  * arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
  *
  * 1. We have pairs: (-, 0.1), (-, 0.4), (+, 0.35), (+, 0.8)
  *
  * 2. Let's sort by score descending: (+, 0.8), (-, 0.4), (+, 0.35), (-, 0.1)
  *
  * 3. Let's draw the points:
  *
  * threshold = 0.8,  TP = 0, FP = 0, FN = 2, Recall = 0.0, Precision = 1
  * threshold = 0.4,  TP = 1, FP = 0, FN = 1, Recall = 0.5, Precision = 1
  * threshold = 0.35, TP = 1, FP = 1, FN = 1, Recall = 0.5, Precision = 0.5
  * threshold = 0.1,  TP = 2, FP = 1, FN = 0, Recall = 1.0, Precision = 0.666
  * threshold = 0,    TP = 2, FP = 2, FN = 0, Recall = 1.0, Precision = 0.5
  *
  * This implementation uses the right Riemann sum (see https://en.wikipedia.org/wiki/Riemann_sum) to calculate the AUC.
  * That is, each increment in area is calculated using `(R_n - R_{n-1}) * P_n`,
  *   where `R_n` is the Recall at the `n`-th point and `P_n` is the Precision at the `n`-th point.
  *
  * This implementation is not interpolated and is different from computing the AUC with the trapezoidal rule,
  *   which uses linear interpolation and can be too optimistic for the Precision Recall AUC metric.
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

        size_t prev_tp = 0, curr_tp = 0;
        size_t curr_p = 0;

        Float64 prev_score = sorted_labels[0].score;
        Float64 curr_precision = 1.0;

        Float64 area = 0.0;

        for (size_t i = 0; i < size; ++i)
        {
            if (sorted_labels[i].score != prev_score)
            {
                /* Precision = TP / (TP + FP)
                 * Recall = TP / (TP + FN)
                 *
                 * Instead of calculating
                 *   d_Area = Precision_n * (Recall_n - Recall_{n-1}),
                 * we can just calculate
                 *   d_Area = Precision_n * (TP_n - TP_{n-1})
                 * and later divide it by (TP + FN).
                 *
                 * This can be done because (TP + FN) is constant and equal to total positive labels.
                 */
                curr_precision = static_cast<Float64>(curr_tp) / curr_p;
                area += curr_precision * (curr_tp - prev_tp);
                prev_tp = curr_tp;
                prev_score = sorted_labels[i].score;
            }

            if (sorted_labels[i].label)
                curr_tp += 1;
            curr_p += 1;
        }

        curr_precision = curr_p > 0 ? static_cast<Float64>(curr_tp) / curr_p : 1.0;
        area += curr_precision * (curr_tp - prev_tp);

        /// If there were no positive labels, Recall did not change and the area is 0
        if (curr_tp == 0)
            return 0.0;
        /// Finally, we divide by (TP + FN) to obtain the Recall
        /// At this point we've traversed the whole curve and curr_tp = total positive labels (TP + FN)
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
