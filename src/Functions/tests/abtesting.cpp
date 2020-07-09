#include <Functions/abtesting.h>
#include <iostream>
#include <stdio.h>

DB::ABTestResult test_bayesab(std::string dist, std::vector<double> xs, std::vector<double> ys, size_t & max, size_t & min)
{
    DB::ABTestResult ret;

    std::cout << std::fixed;
    if (dist == "beta")
    {
        std::cout << dist << "\nclicks: ";
        for (auto x : xs) std::cout << x << " ";

        std::cout <<"\tconversions: ";
        for (auto y : ys) std::cout << y << " ";

        std::cout << "\n";

        ret = DB::bayesian_ab_test<true>(dist, xs, ys);
    }
    else if (dist == "gamma")
    {
        std::cout << dist << "\nclicks: ";
        for (auto x : xs) std::cout << x << " ";

        std::cout <<"\tcost: ";
        for (auto y : ys) std::cout << y << " ";

        std::cout << "\n";
        ret = DB::bayesian_ab_test<false>(dist, xs, ys);
    }

    for (size_t i = 0; i < ret.beats_control.size(); ++i)
        std::cout << i << " beats 0: " << ret.beats_control[i] << std::endl;

    for (size_t i = 0; i < ret.beats_control.size(); ++i)
        std::cout << i << " to be best: " << ret.best[i] << std::endl;

    max = std::max_element(ret.best.begin(), ret.best.end()) - ret.best.begin();
    min = std::min_element(ret.best.begin(), ret.best.end()) - ret.best.begin();

    return ret;
}


int main(int, char **)
{
    size_t max, min;

    auto ret = test_bayesab("beta", {10000, 1000, 900}, {600, 110, 90}, max, min);
    if (max != 1) exit(1);

    ret = test_bayesab("beta", {3000, 3000, 3000}, {600, 100, 90}, max, min);
    if (max != 0) exit(1);

    ret = test_bayesab("beta", {3000, 3000, 3000}, {100, 90, 110}, max, min);
    if (max != 2) exit(1);

    ret = test_bayesab("beta", {3000, 3000, 3000}, {110, 90, 100}, max, min);
    if (max != 0) exit(1);

    ret = test_bayesab("gamma", {10000, 1000, 900}, {600, 110, 90}, max, min);
    if (max != 1) exit(1);

    ret = test_bayesab("gamma", {3000, 3000, 3000}, {600, 100, 90}, max, min);
    if (max != 0) exit(1);

    ret = test_bayesab("gamma", {3000, 3000, 3000}, {100, 90, 110}, max, min);
    if (max != 2) exit(1);

    ret = test_bayesab("gamma", {3000, 3000, 3000}, {110, 90, 100}, max, min);
    if (max != 0) exit(1);
}
