#include <algorithm>
#include <vector>
#include <Functions/FunctionFactory.h>
#include "arrayScalarProduct.h"

namespace DB
{
struct NameAUC
{
    static constexpr auto name = "auc";
};

class AUCImpl
{
public:
    using ResultType = Float64;

    struct ScoreLabel
    {
        ResultType score;
        UInt8 label;
    };

    static DataTypePtr getReturnType(const DataTypePtr & /* nested_type1 */, const DataTypePtr & nested_type2)
    {
        WhichDataType which2(nested_type2);
        if (!which2.isUInt8())
        {
            throw Exception(std::string(NameAUC::name) + "lable type must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    template <typename T, typename U>
    static ResultType apply(
        const PaddedPODArray<T> & scores,
        ColumnArray::Offset score_offset,
        size_t score_len,
        const PaddedPODArray<U> & labels,
        ColumnArray::Offset label_offset,
        size_t label_len)
    {
        if (score_len != label_len)
            throw Exception{"Unmatched length of arrays in " + std::string(NameAUC::name), ErrorCodes::LOGICAL_ERROR};
        if (score_len == 0)
            return {};

        // Order pairs of score and lable by score ascending
        size_t num_pos = 0;
        size_t num_neg = 0;
        std::vector<ScoreLabel> pairs(score_len);
        for (size_t i = 0; i < score_len; ++i)
        {
            pairs[i].score = scores[i + score_offset];
            pairs[i].label = (labels[i + label_offset] ? 1 : 0);
            if (pairs[i].label)
                ++num_pos;
            else
                ++num_neg;
        }
        std::sort(pairs.begin(), pairs.end(), [](const auto & lhs, const auto & rhs) { return lhs.score < rhs.score; });

        // Calculate AUC
        size_t curr_cnt = 0;
        size_t curr_pos_cnt = 0;
        size_t curr_sum = 0;
        ResultType last_score = -1;
        ResultType rank_sum = 0;
        for (size_t i = 0; i < pairs.size(); ++i)
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
        return (rank_sum - num_pos * (num_pos + 1) / 2) / (num_pos * num_neg);
    }
};

/// auc(array_score, array_label) - Calculate AUC with array of score and label
using FunctionAUC = FunctionArrayScalarProduct<AUCImpl, NameAUC>;

void registerFunctionAUC(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAUC>();
}


}