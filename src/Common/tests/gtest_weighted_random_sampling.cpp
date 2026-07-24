#include <Common/WeightedRandomSampling.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <unordered_map>
#include <vector>

using namespace DB;

namespace
{

const size_t N = 1'000'000;
const std::unordered_map<size_t, double> objects = {
    {0, 10},
    {1, 5},
    {2, 5},
    {3, 3},
    {4, 2},
};
const double weights_sum = 25;

class ProbabilityValidator
{
    void calculate(size_t k)
    {
        std::vector<bool> select(objects.size(), false);
        std::fill(select.begin(), select.begin() + k, true);

        do
        {
            std::vector<size_t> subset;
            for (size_t i = 0; i < objects.size(); ++i)
                if (select[i])
                    subset.push_back(i);

            do
            {
                double current_sum = weights_sum;
                double p = 1;

                for (size_t i : subset)
                {
                    p *= (objects.at(i) / current_sum);
                    current_sum -= objects.at(i);
                }

                for (size_t i : subset)
                    probability[i] += p;

            } while (std::next_permutation(subset.begin(), subset.end()));

        } while (std::prev_permutation(select.begin(), select.end()));
    }

public:
    explicit ProbabilityValidator(size_t k) { calculate(k); }

    void validate(std::unordered_map<size_t, size_t> selects)
    {
        constexpr static double ERROR = 0.01;

        ASSERT_EQ(probability.size(), objects.size());
        ASSERT_EQ(selects.size(), objects.size());

        for (size_t i = 0; i < objects.size(); ++i)
        {
            const double algo_prob = 1.0 * selects[i] / N;
            ASSERT_TRUE(probability[i] - ERROR <= algo_prob && algo_prob <= probability[i] + ERROR);
        }
    }

private:
    std::unordered_map<size_t, double> probability;
};

std::unordered_map<size_t, size_t> calculateSelects(size_t k)
{
    std::unordered_map<size_t, size_t> selects_count;
    const auto get_weight = [](const auto & p) { return p.second; };

    for (size_t iter = 0; iter < N; ++iter)
        for (auto it : pickWeightedRandom(objects, get_weight, k))
            selects_count[it->first] += 1;

    return selects_count;
}

}

GTEST_TEST(WeightedRandomSampling, AES1)
{
    ProbabilityValidator(1).validate(calculateSelects(1));
}

GTEST_TEST(WeightedRandomSampling, AES2)
{
    ProbabilityValidator(2).validate(calculateSelects(2));
}

GTEST_TEST(WeightedRandomSampling, AES3)
{
    ProbabilityValidator(3).validate(calculateSelects(3));
}

GTEST_TEST(WeightedRandomSampling, AES4)
{
    ProbabilityValidator(4).validate(calculateSelects(4));
}

GTEST_TEST(WeightedRandomSampling, AES5)
{
    ProbabilityValidator(5).validate(calculateSelects(5));
}
