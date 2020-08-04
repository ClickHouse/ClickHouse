#pragma once

#include <Common/config.h>

#if !defined(ARCADIA_BUILD) && USE_STATS


#include <iostream>
#include <vector>
#include <algorithm>

#include <Core/Types.h>
#include <Common/PODArray.h>


namespace DB
{

typedef struct _Variant
{
    Float64 x;
    Float64 y;
    Float64 beats_control;
    Float64 best;
    std::vector<Float64> samples;

    _Variant(const Float64 _x, const Float64 _y) : x(_x), y(_y), beats_control(0.0), best(0.0), samples() {}
} Variant;

using Variants = PODArray<Variant>;

template <bool higher_is_better>
Variants bayesian_ab_test(String distribution, PODArray<Float64> & xs, PODArray<Float64> & ys);

String convertToJson(const PODArray<String> & variant_names, Variants & variants, const bool include_density);

}

#endif
