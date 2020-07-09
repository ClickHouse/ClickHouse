#pragma once

#include <iostream>
#include <vector>
#include <algorithm>


namespace DB
{

typedef struct _ABTestResult
{
    std::vector<double> beats_control;
    std::vector<double> best;
} ABTestResult;

template <bool higher_is_better>
ABTestResult bayesian_ab_test(std::string distribution, std::vector<double> xs, std::vector<double> ys);

}
