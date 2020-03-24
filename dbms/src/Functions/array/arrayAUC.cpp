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


struct NameArrayAUC
{
    static constexpr auto name = "arrayAUC";
};


class ArrayAUCImpl
{
public:
    using ResultType = Float64;
    using LabelValueSet = std::set<Int16>;
    using LabelValueSets = std::vector<LabelValueSet>;

    inline static const LabelValueSets expect_label_value_sets = {{0, 1}, {-1, 1}};

    struct ScoreLabel
    {
        ResultType score;
        bool label;
    };

    static DataTypePtr getReturnType(const DataTypePtr & /* score_type */, const DataTypePtr & label_type)
    {
        WhichDataType which(label_type);

        // Labels values are either {0, 1} or {-1, 1}, and its type must be one of (Enum8, UInt8, Int8)
        if (!which.isUInt8() && !which.isEnum8() && !which.isInt8())
            throw Exception(std::string(NameArrayAUC::name) + " label type must be UInt8, Enum8 or Int8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <typename T, typename U>
    static ResultType apply(
        const T * scores,
        const U * labels,
        size_t size)
    {
        // Calculate positive labels and restore scores and labels in vector
        size_t num_positive_labels = 0;

        std::vector<ScoreLabel> pairs(size);
        for (size_t i = 0; i < size; ++i)
        {
            pairs[i].score = scores[i];
            pairs[i].label = labels[i] > 0;
            num_positive_labels += pairs[i].label;
        }

        // Order pairs of score and label by score ascending
        std::sort(pairs.begin(), pairs.end(), [](const auto & lhs, const auto & rhs) { return lhs.score < rhs.score; });

        // Calculate AUC
        size_t curr_cnt = 0;
        size_t curr_pos_cnt = 0;
        Int64 curr_sum = 0;
        ResultType last_score = -1;
        ResultType rank_sum = 0;

        for (size_t i = 0; i < size; ++i)
        {
            if (pairs[i].score == last_score)
            {
                curr_sum += i + 1;
                ++curr_cnt;
                if (pairs[i].label)
                    ++curr_pos_cnt;
            }
            else
            {
                if (i > 0)
                    rank_sum += ResultType(curr_sum * curr_pos_cnt) / curr_cnt;

                curr_sum = i + 1;
                curr_cnt = 1;
                curr_pos_cnt = pairs[i].label ? 1 : 0;
            }
            last_score = pairs[i].score;
        }

        rank_sum += ResultType(curr_sum * curr_pos_cnt) / curr_cnt;
        return (rank_sum - num_positive_labels * (num_positive_labels + 1) / 2) / (num_positive_labels * (size - num_positive_labels));
    }
};


/// auc(array_score, array_label) - Calculate AUC with array of score and label
using FunctionArrayAUC = FunctionArrayScalarProduct<ArrayAUCImpl, NameArrayAUC>;

void registerFunctionArrayAUC(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayAUC>();
}
}
