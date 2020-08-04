#pragma once

#include <Common/config.h>

#if !defined(ARCADIA_BUILD) && USE_STATS

#include <vector>

#include <Core/Types.h>
#include <Common/PODArray.h>


namespace DB
{

Float64 approx_linear(const Float64 v, const std::vector<Float64> & x, const std::vector<Float64> & y);
Float64 var(const std::vector<Float64> & x);
Float64 quantile(std::vector<Float64> & x, const Float64 probs);
Float64 nrd(std::vector<Float64> & x);
std::vector<Float64> bin_dist(const std::vector<Float64> & sx, const Float64 sw, const Float64 slo, const Float64 shi, const size_t sn);
void density(std::vector<Float64> & x, std::vector<Float64> & result_x, std::vector<Float64> & result_y);

}

#endif
