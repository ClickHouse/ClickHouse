#include <Common/WRS_algorithm.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <unordered_map>
#include <vector>
#include <array>

using namespace DB;

namespace
{

constexpr size_t N = 1'000'000;
constexpr std::array<double, 5> weights = {10, 5, 5, 3, 2};
constexpr double weights_sum = std::accumulate(weights.begin(), weights.end(), 0.0);

class ProbabilityValidator
{
    void calculate(size_t k)
    {
        std::vector<bool> select(weights.size(), false);
        std::fill(select.begin(), select.begin() + k, true);

        do {
            std::vector<size_t> subset;
            for (size_t i = 0; i < weights.size(); ++i) {
                if (select[i]) {
                    subset.push_back(i);
                }
            }

            do {
                double current_sum = weights_sum;
                double p = 1;

                for (size_t i : subset) {
                    p *= (weights[i] / current_sum);
                    current_sum -= weights[i];
                }

                for (size_t i : subset) {
                    probability[i] += p;
                }

            } while (std::next_permutation(subset.begin(), subset.end()));

        } while (std::prev_permutation(select.begin(), select.end()));
    }

public:
    explicit ProbabilityValidator(size_t k)
    {
        calculate(k);
    }

    void validate(std::unordered_map<size_t, size_t> selects)
    {
        for (size_t i = 0; i < 5; ++i)
        {
            std::cout << 1.0 * selects[i] / N << " " << probability[i] << std::endl;
        }

        constexpr static double ERROR = 0.01;

        ASSERT_EQ(probability.size(), weights.size());
        ASSERT_EQ(selects.size(), weights.size());

        for (size_t i = 0; i < weights.size(); ++i)
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

    for (size_t iter = 0; iter < N; ++iter)
        for (auto index : pickWeightedRandom(weights, k))
            selects_count[index] += 1;

    return selects_count;
}

}

GTEST_TEST(WRS, AES_K_1)
{
    ProbabilityValidator(1).validate(calculateSelects(1));
}

GTEST_TEST(WRS, AES_K_2)
{
    ProbabilityValidator(2).validate(calculateSelects(2));
}

GTEST_TEST(WRS, AES_K_3)
{
    ProbabilityValidator(3).validate(calculateSelects(3));
}

GTEST_TEST(WRS, AES_K_4)
{
    ProbabilityValidator(4).validate(calculateSelects(4));
}

GTEST_TEST(WRS, AES_K_5)
{
    ProbabilityValidator(5).validate(calculateSelects(5));
}

