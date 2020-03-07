#include <algorithm>
#include <vector>
#include <Functions/FunctionFactory.h>
#include "arrayScalarProduct.h"

namespace DB
{
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
        {
            throw Exception(
                std::string(NameArrayAUC::name) + "lable type must be UInt8, Enum8 or Int8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (which.isEnum8())
        {
            auto type8 = checkAndGetDataType<DataTypeEnum8>(label_type.get());
            if (!type8)
                throw Exception(std::string(NameArrayAUC::name) + "lable type not valid Enum8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            LabelValueSet value_set;
            const auto & values = type8->getValues();
            for (const auto & value : values)
                value_set.insert(value.second);

            if (std::find(expect_label_value_sets.begin(), expect_label_value_sets.end(), value_set) == expect_label_value_sets.end())
                throw Exception(
                    std::string(NameArrayAUC::name) + "lable values must be {0, 1} or {-1, 1}", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
            throw Exception{"Unmatched length of arrays in " + std::string(NameArrayAUC::name), ErrorCodes::LOGICAL_ERROR};
        if (score_len == 0)
            return {};

        // Calculate positive and negative label number and restore scores and labels in vector
        size_t num_pos = 0;
        size_t num_neg = 0;
        LabelValueSet label_value_set;
        std::vector<ScoreLabel> pairs(score_len);
        for (size_t i = 0; i < score_len; ++i)
        {
            pairs[i].score = scores[i + score_offset];
            pairs[i].label = (labels[i + label_offset] == 1);
            if (pairs[i].label)
                ++num_pos;
            else
                ++num_neg;

            label_value_set.insert(labels[i + label_offset]);
        }

        // Label values must be {0, 1} or {-1, 1}
        if (std::find(expect_label_value_sets.begin(), expect_label_value_sets.end(), label_value_set) == expect_label_value_sets.end())
            throw Exception(
                std::string(NameArrayAUC::name) + "lable values must be {0, 1} or {-1, 1}", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        // Order pairs of score and lable by score ascending
        std::sort(pairs.begin(), pairs.end(), [](const auto & lhs, const auto & rhs) { return lhs.score < rhs.score; });

        // Calculate AUC
        size_t curr_cnt = 0;
        size_t curr_pos_cnt = 0;
        Int64 curr_sum = 0;
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
using FunctionArrayAUC = FunctionArrayScalarProduct<ArrayAUCImpl, NameArrayAUC>;

void registerFunctionArrayAUC(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayAUC>();
}
}
