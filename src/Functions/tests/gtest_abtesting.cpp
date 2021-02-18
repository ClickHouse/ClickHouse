#include <gtest/gtest.h>

#include <Functions/abtesting.h>
#include <iostream>
#include <stdio.h>

using namespace DB;

Variants test_bayesab(std::string dist, PODArray<Float64> xs, PODArray<Float64> ys, size_t & max, size_t & min)
{
    Variants variants;

    std::cout << std::fixed;
    if (dist == "beta")
    {
        std::cout << dist << "\nclicks: ";
        for (auto x : xs) std::cout << x << " ";

        std::cout <<"\tconversions: ";
        for (auto y : ys) std::cout << y << " ";

        std::cout << "\n";

        variants = bayesian_ab_test<true>(dist, xs, ys);
    }
    else if (dist == "gamma")
    {
        std::cout << dist << "\nclicks: ";
        for (auto x : xs) std::cout << x << " ";

        std::cout <<"\tcost: ";
        for (auto y : ys) std::cout << y << " ";

        std::cout << "\n";
        variants = bayesian_ab_test<true>(dist, xs, ys);
    }

    for (size_t i = 0; i < variants.size(); ++i)
        std::cout << i << " beats 0: " << variants[i].beats_control << std::endl;

    for (size_t i = 0; i < variants.size(); ++i)
        std::cout << i << " to be best: " << variants[i].best << std::endl;

    std::cout << convertToJson({"0", "1", "2"}, variants) << std::endl;

    Float64 max_val = 0.0, min_val = 2.0;
    for (size_t i = 0; i < variants.size(); ++i)
    {
        if (variants[i].best > max_val)
        {
            max_val = variants[i].best;
            max = i;
        }

        if (variants[i].best < min_val)
        {
            min_val = variants[i].best;
            min = i;
        }
    }

    return variants;
}


TEST(BayesAB, beta)
{
    size_t max = 0, min = 0;

    auto variants = test_bayesab("beta", {10000, 1000, 900}, {600, 110, 90}, max, min);
    ASSERT_EQ(1, max);

    variants = test_bayesab("beta", {3000, 3000, 3000}, {600, 100, 90}, max, min);
    ASSERT_EQ(0, max);

    variants = test_bayesab("beta", {3000, 3000, 3000}, {100, 90, 110}, max, min);
    ASSERT_EQ(2, max);

    variants = test_bayesab("beta", {3000, 3000, 3000}, {110, 90, 100}, max, min);
    ASSERT_EQ(0, max);
}


TEST(BayesAB, gamma)
{
    size_t max = 0, min = 0;
    auto variants = test_bayesab("gamma", {10000, 1000, 900}, {600, 110, 90}, max, min);
    ASSERT_EQ(1, max);

    variants = test_bayesab("gamma", {3000, 3000, 3000}, {600, 100, 90}, max, min);
    ASSERT_EQ(0, max);

    variants = test_bayesab("gamma", {3000, 3000, 3000}, {100, 90, 110}, max, min);
    ASSERT_EQ(2, max);

    variants = test_bayesab("gamma", {3000, 3000, 3000}, {110, 90, 100}, max, min);
    ASSERT_EQ(0, max);
}

