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
  * FPR = countIf(score > score_i, label = negative) = count negative labels above certain score
  * TPR = countIf(score > score_i, label = positive) = count positive labels above certain score
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
        WhichDataType which(label_type);

        if (!isNumber(which))
            throw Exception(std::string(NameArrayAUC::name) + " label must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <typename T, typename U>
    static ResultType apply(
        const T * scores,
        const U * labels,
        size_t size)
    {
        // Calculate positive labels and restore scores and labels in vector
        size_t total_positive_labels = 0;

        struct ScoreLabel
        {
            T score;
            bool label;
        };

        std::vector<ScoreLabel> sorted_labels(size); /// TODO Memory allocation in inner loop - does it really needed?

        for (size_t i = 0; i < size; ++i)
        {
            bool label = labels[i] > 0;
            sorted_labels[i].score = scores[i];
            sorted_labels[i].label = label;
            total_positive_labels += label;
        }

        // Order pairs of score and label by score ascending
        std::sort(sorted_labels.begin(), sorted_labels.end(), [](const auto & lhs, const auto & rhs) { return lhs.score < rhs.score; });

        // Calculate the AUC
        size_t curr_cnt = 0;
        size_t cumulative_count_of_positive = 0;
        Int64 curr_sum = 0;
        T last_score = -1;
        ResultType rank_sum = 0;

        for (size_t i = 0; i < size; ++i)
        {
            auto score = sorted_labels[i].score;
            bool label = sorted_labels[i].label;

            if (score == last_score)
            {
                curr_sum += i + 1;
                ++curr_cnt;
                if (label)
                    ++cumulative_count_of_positive;
            }
            else
            {
                if (i > 0)
                    rank_sum += ResultType(curr_sum * cumulative_count_of_positive) / curr_cnt;

                curr_sum = i + 1;
                curr_cnt = 1;
                cumulative_count_of_positive = label;
            }
            last_score = score;
        }

        rank_sum += ResultType(curr_sum * cumulative_count_of_positive) / curr_cnt;
        return (rank_sum - total_positive_labels * (total_positive_labels + 1) / ResultType(2)) / (total_positive_labels * (size - total_positive_labels));
    }
};


/// auc(array_score, array_label) - Calculate AUC with array of score and label
using FunctionArrayAUC = FunctionArrayScalarProduct<ArrayAUCImpl, NameArrayAUC>;

void registerFunctionArrayAUC(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayAUC>();
}
}
