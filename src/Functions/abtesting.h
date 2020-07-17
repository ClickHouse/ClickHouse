#if !defined(ARCADIA_BUILD)
#pragma once

#include <iostream>
#include <vector>
#include <algorithm>


namespace DB
{

typedef struct _Variant
{
    double beats_control;
    double best;
    std::vector<double> samples;
} Variant;

using Variants = std::vector<Variant>;

template <bool higher_is_better>
Variants bayesian_ab_test(std::string distribution, std::vector<double> xs, std::vector<double> ys);

}
#endif
