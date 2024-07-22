#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
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
  * This way classifier may predict positive or negative value correctly - true positive or true negative
  *   or have false positive or false negative result.
  * Verying the threshold we can get different probabilities of false positive or false negatives or true positives, etc...
  *
  * We can also calculate the True Positive Rate and the False Positive Rate:
  *
  * TPR (also called "sensitivity", "recall" or "probability of detection")
  *  is the probability of classifier to give positive result if the object has positive label:
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
  * arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
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
  */

class FunctionArrayAUC : public IFunction
{
public:
    static constexpr auto name = "arrayAUC";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayAUC>(); }

private:
    static Float64 apply(
        const IColumn & scores,
        const IColumn & labels,
        ColumnArray::Offset current_offset,
        ColumnArray::Offset next_offset)
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

        /// Stable sort is required for for labels to apply in same order if score is equal
        std::stable_sort(sorted_labels.begin(), sorted_labels.end(), [](const auto & lhs, const auto & rhs) { return lhs.score > rhs.score; });

        /// We will first calculate non-normalized area.

        size_t area = 0;
        size_t count_positive = 0;
        for (size_t i = 0; i < size; ++i)
        {
            if (sorted_labels[i].label)
                ++count_positive; /// The curve moves one step up. No area increase.
            else
                area += count_positive; /// The curve moves one step right. Area is increased by 1 * height = count_positive.
        }

        /// Then divide the area to the area of rectangle.

        if (count_positive == 0 || count_positive == size)
            return std::numeric_limits<Float64>::quiet_NaN();

        return static_cast<Float64>(area) / count_positive / (size - count_positive);
    }

    static void vector(
        const IColumn & scores,
        const IColumn & labels,
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<Float64> & result)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto next_offset = offsets[i];
            result[i] = apply(scores, labels, current_offset, next_offset);
            current_offset = next_offset;
        }
    }

public:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < getNumberOfArguments(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "All arguments for function {} must be an array.", getName());

            const auto & nested_type = array_type->getNestedType();
            if (!isNativeNumber(nested_type) && !isEnum(nested_type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} cannot process values of type {}",
                                getName(), nested_type->getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnPtr col1 = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr col2 = arguments[1].column->convertToFullColumnIfConst();

        const ColumnArray * col_array1 = checkAndGetColumn<ColumnArray>(col1.get());
        if (!col_array1)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

        const ColumnArray * col_array2 = checkAndGetColumn<ColumnArray>(col2.get());
        if (!col_array2)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second argument of function {}", arguments[1].column->getName(), getName());

        if (!col_array1->hasEqualOffsets(*col_array2))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Array arguments for function {} must have equal sizes", getName());

        auto col_res = ColumnVector<Float64>::create();

        vector(
            col_array1->getData(),
            col_array2->getData(),
            col_array1->getOffsets(),
            col_res->getData());

        return col_res;
    }
};


REGISTER_FUNCTION(ArrayAUC)
{
    factory.registerFunction<FunctionArrayAUC>();
}

}
