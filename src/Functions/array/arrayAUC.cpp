#include <algorithm>
#include <vector>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include "arrayScalarProduct.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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


struct NameArrayAUC
{
    static constexpr auto name = "arrayAUC";
};


class ArrayAUCImpl
{
public:
    using ResultType = Float64;

    static DataTypePtr getReturnType(const DataTypePtr & /* score_type */, const DataTypePtr & label_type)
    {
        if (!(isNumber(label_type) || isEnum(label_type)))
            throw Exception(std::string(NameArrayAUC::name) + " label must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <typename T, typename U>
    static ResultType apply(
        const T * scores,
        const U * labels,
        size_t size)
    {
        struct ScoreLabel
        {
            T score;
            bool label;
        };

        PODArrayWithStackMemory<ScoreLabel, 1024> sorted_labels(size);

        for (size_t i = 0; i < size; ++i)
        {
            bool label = labels[i] > 0;
            sorted_labels[i].score = scores[i];
            sorted_labels[i].label = label;
        }

        std::sort(sorted_labels.begin(), sorted_labels.end(), [](const auto & lhs, const auto & rhs) { return lhs.score > rhs.score; });

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
            return std::numeric_limits<ResultType>::quiet_NaN();

        return ResultType(area) / count_positive / (size - count_positive);
    }
};


/// auc(array_score, array_label) - Calculate AUC with array of score and label
using FunctionArrayAUC = FunctionArrayScalarProduct<ArrayAUCImpl, NameArrayAUC>;

void registerFunctionArrayAUC(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayAUC>();
}
}
