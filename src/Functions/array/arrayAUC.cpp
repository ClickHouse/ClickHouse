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
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** The function takes two arrays: scores and labels.
  * Label can be one of two values: positive (> 0) and negative (<= 0)
  * Score can be arbitrary number.
  *
  * These values are considered as the output of classifier. We have some true labels for objects.
  * And classifier assigns some scores to objects that predict these labels in the following way:
  * - we can define arbitrary threshold on score and predict that the label is positive if the score is greater than the threshold:
  *
  * f(object) = score
  * predicted_label = score > threshold
  *
  * This way classifier may predict positive or negative value correctly - true positive or true negative
  *   or have false positive or false negative result.
  * Verying the threshold we can get different probabilities of false positive or false negatives or true positives, etc...
  *
  * ---------------------------------------------------------------------------------------------------------------------
  *
  * Area Under the Receiver Operating Characteristic (ROC) curve
  *
  * The ROC curve plots True Positive Rate (TPR) x False Positive Rate (FPR):
  *
  * TPR (also called "sensitivity", "recall" or "probability of detection")
  *   is the probability of classifier to give positive result if the object has positive label:
  * TPR = P(score > threshold | label = positive)
  *
  * FPR is the probability of classifier to give positive result if the object has negative label:
  * FPR = P(score > threshold | label = negative)
  *
  * We can draw a curve of values of FPR and TPR with different threshold on [0..1] x [0..1] unit square.
  * This curve is named "ROC curve" (Receiver Operating Characteristic).
  *
  * For ROC we can calculate, literally, Area Under the Curve, that will be in the range of [0..1].
  * The higher the AUC the better the classifier.
  *
  * AUC also is as the probability that the score for positive label is greater than the score for negative label.
  *
  * https://developers.google.com/machine-learning/crash-course/classification/roc-and-auc
  * https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve
  *
  * To calculate AUC, we will draw points of (FPR, TPR) for different thresholds = score_i.
  * FPR_raw = countIf(score > score_i, label = negative) = count negative labels above certain score
  * TPR_raw = countIf(score > score_i, label = positive) = count positive labels above certain score
  *
  * Let's look at the example:
  * arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
  *
  * 1. We have pairs: (-, 0.1), (-, 0.4), (+, 0.35), (+, 0.8)
  *
  * 2. Let's sort by score: (-, 0.1), (+, 0.35), (-, 0.4), (+, 0.8)
  *
  * 3. Let's draw the points:
  *
  * threshold = 0,    TPR = 1,   FPR = 1,   TPR_raw = 2, FPR_raw = 2
  * threshold = 0.1,  TPR = 1,   FPR = 0.5, TPR_raw = 2, FPR_raw = 1
  * threshold = 0.35, TPR = 0.5, FPR = 0.5, TPR_raw = 1, FPR_raw = 1
  * threshold = 0.4,  TPR = 0.5, FPR = 0,   TPR_raw = 1, FPR_raw = 0
  * threshold = 0.8,  TPR = 0,   FPR = 0,   TPR_raw = 0, FPR_raw = 0
  *
  * The "curve" will be present by a line that moves one step either towards right or top on each threshold change.
  *
  * This implementation uses the trapezoidal rule (https://en.wikipedia.org/wiki/Trapezoidal_rule) to calculate the AUC,
  * That is, each increment in area is calculated using `(FPR_n - FPR_{n-1}) * (TPR_n + TPR_{n-1}) / 2`,
  *   where `FPR_n` is the FPR at the `n`-th point and `TPR_n` is the TPR at the `n`-th point.
  *
  * ---------------------------------------------------------------------------------------------------------------------
  *
  * Area Under the Precision-Recall (PR) curve
  *
  * The PR curve plots Precision x Recall:
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
  * arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
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

template <bool is_pr>
class FunctionArrayAUC : public IFunction
{
public:
    static constexpr auto name = is_pr ? "arrayAUCPR" : "arrayROCAUC";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayAUC<is_pr>>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t argument_count = arguments.size();

        if ((is_pr && (argument_count < 2 || argument_count > 3))
            || (!is_pr && (argument_count < 2 || argument_count > 4)))
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be {}",
                getName(),
                argument_count,
                is_pr ? "2 or 3" : "2, 3 or 4");

        /// Validate 'arr_scores' argument
        const DataTypeArray * array_scores_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!array_scores_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument (arr_scores) for function {} must be of type Array", getName());

        const auto & nested_array_scores_type = array_scores_type->getNestedType();
        if (!isNativeNumber(nested_array_scores_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument (arr_scores) for function {} is of type {}, should be a number",
                getName(),
                nested_array_scores_type->getName());

        /// Validate 'arr_labels' argument
        const DataTypeArray * array_labels_type = checkAndGetDataType<DataTypeArray>(arguments[1].type.get());
        if (!array_labels_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument (arr_labels) for function {} must be of type Array", getName());

        const auto & nested_arary_labels_type = array_labels_type->getNestedType();
        if (!isNativeNumber(nested_arary_labels_type) && !isEnum(nested_arary_labels_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument (arr_labels) for function {} is of type {}, should be a number or Enum",
                getName(),
                nested_arary_labels_type->getName());

        /// Validate 'scale' argument
        if (!is_pr && argument_count >= 3 && !isConstBoolColumn(arguments[2]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument (scale) for function {} must be of type const Bool", getName());

        /// Validate 'arr_partial_offsets' argument
        if (argument_count >= array_partial_offsets_arg_index + 1)
        {
            const DataTypeArray * array_offsets_type = checkAndGetDataType<DataTypeArray>(arguments[array_partial_offsets_arg_index].type.get());
            if (!array_offsets_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "{} argument (arr_partial_offsets) for function {} must be of type Array",
                    array_partial_offsets_arg_index == 2 ? "Third" : "Fourth",
                    getName());

            const auto & nested_array_offsets_type = array_offsets_type->getNestedType();
            /// Last argument (arr_partial_offsets) must be an array of integers
            if (!isInteger(nested_array_offsets_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "{} argument (arr_partial_offsets) for function {} is of type {}, should be integer",
                    array_partial_offsets_arg_index == 2 ? "Third" : "Fourth",
                    getName(),
                    nested_array_offsets_type->getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeFloat64>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t argument_count = arguments.size();

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First two arguments for function {} must be Arrays of equal sizes", getName());

        /// Handle the 'scale' argument (if passed and the function is arrayROCAUC, otherwise default to true)
        bool scale = true;
        if (!is_pr && argument_count >= 3 && input_rows_count > 0)
            scale = arguments[2].column->getBool(0);

        /// Handle the 'arr_partial_offsets' argument (if passed)
        ColumnPtr col_offsets = nullptr;
        const ColumnArray * col_array_offsets = nullptr;
        if (argument_count == array_partial_offsets_arg_index + 1)
        {
            col_offsets = arguments[array_partial_offsets_arg_index].column->convertToFullColumnIfConst();
            col_array_offsets = checkAndGetColumn<ColumnArray>(col_offsets.get());
            if (!col_array_offsets)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of {} argument of function {}",
                    arguments[array_partial_offsets_arg_index].column->getName(),
                    array_partial_offsets_arg_index == 2 ? "third" : "fourth",
                    getName());

            /// The partial offsets argument must be a column containing 3-elements (PR AUC) or 4-elements (ROC AUC) arrays on each row
            const auto & offsets = col_array_offsets->getOffsets();

            if ((col_array_offsets->getData().size() != array_partial_offsets_size * input_rows_count) || (offsets.size() != input_rows_count))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{} argument (arr_partial_offsets) for function {} must contain Arrays of size {}",
                    array_partial_offsets_arg_index == 2 ? "Third" : "Fourth",
                    getName(),
                    array_partial_offsets_size);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto current = offsets[i];
                auto previous = i == 0 ? 0 : offsets[i - 1];
                if (current - previous != array_partial_offsets_size)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "{} argument (arr_partial_offsets) for function {} must contain Arrays of size {}, not {}",
                        array_partial_offsets_arg_index == 2 ? "Third" : "Fourth",
                        getName(),
                        array_partial_offsets_size,
                        current - previous);
            }

            for (size_t i = 0; i < col_array_offsets->getData().size(); ++i)
            {
                if (col_array_offsets->getData().getInt(i) < 0)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "{} argument (arr_partial_offsets) for function {} must not contain negative values",
                        array_partial_offsets_arg_index == 2 ? "Third" : "Fourth",
                        getName());
            }
        }

        auto col_res = ColumnVector<Float64>::create();

        vector(
            col_array1->getData(),
            col_array2->getData(),
            col_array1->getOffsets(),
            col_res->getData(),
            input_rows_count,
            scale,
            col_array_offsets);

        return col_res;
    }
private:
    static constexpr size_t array_partial_offsets_arg_index = is_pr ? 2 : 3;
    static constexpr size_t array_partial_offsets_size = is_pr ? 3 : 4;

    static bool isConstBoolColumn(ColumnWithTypeAndName argument)
    {
        if (!isBool(argument.type))
            return false;
        if (argument.column.get() == nullptr)
            return false;
        if (!isColumnConst(*argument.column))
            return false;
        return true;
    }

    static Float64 increase_unscaled_area(size_t prev_fp, size_t prev_tp, size_t curr_fp, size_t curr_tp)
    {
        if constexpr (is_pr)
            /// PR curve plots Precision x Recall
            ///
            /// Precision = TP / (TP + FP)
            /// Recall = TP / (TP + FN)
            ///
            /// The AUC is calculated using the Right Riemann Sum.
            ///
            /// Instead of calculating
            ///   area += (Recall_n - Recall_{n-1}) * Precision_n,
            /// we simplify it to
            ///   area += (TP_n - TP_{n-1}) * Precision_n
            /// and compute the "unscaled" area.
            ///
            /// The unscaled area represents the AUC of the Precision x TP curve.
            /// Later we can divide it by (TP + FN) to obtain the correct AUC.
            ///
            /// This can be done because (TP + FN) is constant and equal to total positive labels.
            return static_cast<Float64>(curr_tp) / (curr_tp + curr_fp) * (curr_tp - prev_tp);
        else
            /// ROC curve plots TPR x FPR
            ///
            /// TPR = TP / (TP + FN)
            /// FPR = FP / (FP + TN)
            ///
            /// The AUC is calculated using the Trapezoidal Rule.
            ///
            /// Instead of calculating
            ///   area += (FPR_n - FPR_{n-1}) * (TPR_n + TPR_{n-1}) / 2,
            /// we simplify it to
            ///   area += (FP_n - FP_{n-1}) * (TP_n + TP_{n-1}) / 2,
            /// and compute the "unscaled" area.
            ///
            /// The unscaled area represents the AUC of the TP x FP curve.
            /// Later we can divide it by (TP + FN) and (FP + TN) to obtain the correct AUC.
            ///
            /// This can be done because both (TP + FN) and (FP + TN) are constant and
            ///   equal to total positive labels and total negative labels, respectively.
            return (curr_fp - prev_fp) * (curr_tp + prev_tp) / 2.0;
    }

    static Float64 scale_back_area(Float64 area, size_t total_positive_labels, size_t total_negative_labels)
    {
        if constexpr (is_pr)
            /// To simplify the calculations, previously we calculated the AUC for the Precision x TP curve.
            /// This scales back to Precision x Recall by dividing the area by (TP + FN).
            return area / total_positive_labels;
        else
            /// To simplify the calculations, previously we calculated the AUC for the TP x FP curve.
            /// This scales back to TPR x FPR by dividing the area by (TP + FN) and (FP + TN).
            return area / total_positive_labels / total_negative_labels;
    }

    static Float64 apply(
        const IColumn & scores,
        const IColumn & labels,
        ColumnArray::Offset current_offset,
        ColumnArray::Offset next_offset,
        bool scale,
        size_t higher_partitions_tp = 0,
        size_t higher_partitions_fp = 0,
        size_t total_positives = 0,
        size_t total_negatives = 0)
    {
        struct ScoreLabel
        {
            Float64 score;
            bool label;
        };

        size_t size = next_offset - current_offset;

        if (size == 0)
            return is_pr ? 0.0 : std::numeric_limits<Float64>::quiet_NaN();

        PODArrayWithStackMemory<ScoreLabel, 1024> sorted_labels(size);

        for (size_t i = 0; i < size; ++i)
        {
            sorted_labels[i].label = labels.getFloat64(current_offset + i) > 0;
            sorted_labels[i].score = scores.getFloat64(current_offset + i);
        }

        /// Sorting scores in descending order to traverse the ROC / Precision-Recall curve from left to right
        std::sort(sorted_labels.begin(), sorted_labels.end(), [](const auto & lhs, const auto & rhs) { return lhs.score > rhs.score; });

        Float64 area = 0.0;
        Float64 threshold = sorted_labels[0].score;

        size_t prev_fp = higher_partitions_fp;
        size_t prev_tp = higher_partitions_tp;
        size_t curr_fp = higher_partitions_fp; /// False positives predictions (label <= 0 and score > threshold)
        size_t curr_tp = higher_partitions_tp; /// True positives predictions (label > 0 and score > threshold)

        /// Traversing the sorted labels, changing the threshold and incrementing the area accordingly
        for (size_t i = 0; i < size; ++i)
        {
            /// Only increment the area when the threshold (score) changes
            if (sorted_labels[i].score != threshold)
            {
                area += increase_unscaled_area(prev_fp, prev_tp, curr_fp, curr_tp);
                prev_fp = curr_fp;
                prev_tp = curr_tp;
                threshold = sorted_labels[i].score;
            }

            if (sorted_labels[i].label)
                curr_tp += 1;
            else
                curr_fp += 1;
        }

        area += increase_unscaled_area(prev_fp, prev_tp, curr_fp, curr_tp);

        /// Unless scale is false, we scale the area back to the [0..1] range
        if (scale)
        {
            /// Degenerate cases where we would divide by zero when scaling back the area
            if (!is_pr && ((total_positives == 0 && curr_tp == 0) || (total_negatives == 0 && curr_fp == 0)))
                /// If no positive or negative labels, TPR or FPR is undefined and we return NaN
                return std::numeric_limits<Float64>::quiet_NaN();
            if (is_pr && (total_positives == 0 && curr_tp == 0))
                /// Precision did not change, PR curve degenerates into a single point (0, 1) and we return 0.0
                return 0.0;

            /// If we're calculating the partial AUC, the user needs to pass the total_positives or total_negatives labels
            /// Otherwise, we assume that the whole curve was traversed above, which means that the threshold is at minimum and all
            /// labels were predicted as true. This means that curr_tp = total positive labels and curr_fp = total negative labels
            return scale_back_area(
                area, total_positives != 0 ? total_positives : curr_tp, total_negatives != 0 ? total_negatives : curr_fp);
        }
        return area;
    }

    static void vector(
        const IColumn & scores,
        const IColumn & labels,
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<Float64> & result,
        size_t input_rows_count,
        bool scale,
        const ColumnArray * partial_auc_offsets)
    {
        result.resize(input_rows_count);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto next_offset = offsets[i];
            if (partial_auc_offsets)
                result[i] = apply(
                    scores,
                    labels,
                    current_offset,
                    next_offset,
                    scale,
                    partial_auc_offsets->getData().getUInt(array_partial_offsets_size * i),
                    partial_auc_offsets->getData().getUInt(array_partial_offsets_size * i + 1),
                    partial_auc_offsets->getData().getUInt(array_partial_offsets_size * i + 2),
                    array_partial_offsets_size == 4 ? partial_auc_offsets->getData().getUInt(array_partial_offsets_size * i + 3) : 0);
            else
                result[i] = apply(scores, labels, current_offset, next_offset, scale);
            current_offset = next_offset;
        }
    }
};


REGISTER_FUNCTION(ArrayAUC)
{
    /// ROC AUC
    FunctionDocumentation::Description description_roc = R"(
Calculates the area under the receiver operating characteristic (ROC) curve.
A ROC curve is created by plotting True Positive Rate (TPR) on the y-axis and False Positive Rate (FPR) on the x-axis across all thresholds.
The resulting value ranges from zero to one, with a higher value indicating better model performance.

The ROC AUC (also known as simply AUC) is a concept in machine learning.
For more details, please see [here](https://developers.google.com/machine-learning/glossary#pr-auc-area-under-the-pr-curve), [here](https://developers.google.com/machine-learning/crash-course/classification/roc-and-auc#expandable-1) and [here](https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).
)";
    FunctionDocumentation::Syntax syntax_roc = "arrayROCAUC(scores, labels[, scale[, partial_offsets]])";
    FunctionDocumentation::Arguments arguments_roc = {
        {"scores", "Scores prediction model gives.", {"Array((U)Int*)", "Array(Float*)"}},
        {"labels", "Labels of samples, usually 1 for positive sample and 0 for negative sample.", {"Array((U)Int*)", "Enum"}},
        {"scale", "Optional. Decides whether to return the normalized area. If false, returns the area under the TP (true positives) x FP (false positives) curve instead. Default value: true.", {"Bool"}},
        {"partial_offsets", R"(
- An array of four non-negative integers for calculating a partial area under the ROC curve (equivalent to a vertical band of the ROC space) instead of the whole AUC. This option is useful for distributed computation of the ROC AUC. The array must contain the following elements [`higher_partitions_tp`, `higher_partitions_fp`, `total_positives`, `total_negatives`]. [Array](/sql-reference/data-types/array) of non-negative [Integers](../data-types/int-uint.md). Optional.
    - `higher_partitions_tp`: The number of positive labels in the higher-scored partitions.
    - `higher_partitions_fp`: The number of negative labels in the higher-scored partitions.
    - `total_positives`: The total number of positive samples in the entire dataset.
    - `total_negatives`: The total number of negative samples in the entire dataset.

::::note
When `arr_partial_offsets` is used, the `arr_scores` and `arr_labels` should be only a partition of the entire dataset, containing an interval of scores.
The dataset should be divided into contiguous partitions, where each partition contains the subset of the data whose scores fall within a specific range.
For example:
- One partition could contain all scores in the range [0, 0.5).
- Another partition could contain scores in the range [0.5, 1.0].
::::
)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_roc = {"Returns area under the receiver operating characteristic (ROC) curve.", {"Float64"}};
    FunctionDocumentation::Examples examples_roc = {{"Usage example", "SELECT arrayROCAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);", "0.75"}};
    FunctionDocumentation::IntroducedIn introduced_in_roc = {20, 4};
    FunctionDocumentation::Category category_roc = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_roc = {description_roc, syntax_roc, arguments_roc, returned_value_roc, examples_roc, introduced_in_roc, category_roc};

    factory.registerFunction<FunctionArrayAUC<false>>(documentation_roc);
    factory.registerAlias("arrayAUC", "arrayROCAUC"); /// Backward compatibility, also ROC AUC is often shorted to just AUC

    /// PR AUC
    FunctionDocumentation::Description description_pr = R"(
Calculates the area under the precision-recall (PR) curve.
A precision-recall curve is created by plotting precision on the y-axis and recall on the x-axis across all thresholds.
The resulting value ranges from 0 to 1, with a higher value indicating better model performance.
The PR AUC is particularly useful for imbalanced datasets, providing a clearer comparison of performance compared to ROC AUC on those cases.
For more details, please see [here](https://developers.google.com/machine-learning/glossary#pr-auc-area-under-the-pr-curve), [here](https://developers.google.com/machine-learning/crash-course/classification/roc-and-auc#expandable-1) and [here](https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).
)";
    FunctionDocumentation::Syntax syntax_pr = "arrayAUCPR(scores, labels[, partial_offsets])";
    FunctionDocumentation::Arguments arguments_pr = {
        {"cores", "Scores prediction model gives.", {"Array((U)Int*)", "Array(Float*)"}},
        {"labels", "Labels of samples, usually 1 for positive sample and 0 for negative sample.", {"Array((U)Int*)", "Array(Enum)"}},
        {"partial_offsets", R"(
- Optional. An [`Array(T)`](/sql-reference/data-types/array) of three non-negative integers for calculating a partial area under the PR curve (equivalent to a vertical band of the PR space) instead of the whole AUC. This option is useful for distributed computation of the PR AUC. The array must contain the following elements [`higher_partitions_tp`, `higher_partitions_fp`, `total_positives`].
    - `higher_partitions_tp`: The number of positive labels in the higher-scored partitions.
    - `higher_partitions_fp`: The number of negative labels in the higher-scored partitions.
    - `total_positives`: The total number of positive samples in the entire dataset.

::::note
When `arr_partial_offsets` is used, the `arr_scores` and `arr_labels` should be only a partition of the entire dataset, containing an interval of scores.
The dataset should be divided into contiguous partitions, where each partition contains the subset of the data whose scores fall within a specific range.
For example:
- One partition could contain all scores in the range [0, 0.5).
- Another partition could contain scores in the range [0.5, 1.0].
::::
)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_pr = {"Returns area under the precision-recall (PR) curve.", {"Float64"}};
    FunctionDocumentation::Examples examples_pr = {{"Usage example", "SELECT arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);", R"(
┌─arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                              0.8333333333333333 │
└─────────────────────────────────────────────────┘
)"}};
    FunctionDocumentation::IntroducedIn introduced_in_pr = {20, 4};
    FunctionDocumentation::Category category_pr = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_pr = {description_pr, syntax_pr, arguments_pr, returned_value_pr, examples_pr, introduced_in_pr, category_pr};

    factory.registerFunction<FunctionArrayAUC<true>>(documentation_pr);
    factory.registerAlias("arrayPRAUC", "arrayAUCPR");
}

}
