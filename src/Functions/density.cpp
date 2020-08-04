/*
 *  This file is copied from R core to avoid including whole R core.
 *
 *  Copyright (C) 1997--2019   The R Core Team
 *  Copyright (C) 1995, 1996   Robert Gentleman and Ross Ihaka
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, a copy is available at
 *  https://www.R-project.org/Licenses/
 */
#include <Functions/density.h>

#if !defined(ARCADIA_BUILD) && USE_STATS

#include <math.h>
#include <float.h>
#include <complex>
#include <algorithm>

#define STATS_ENABLE_STDVEC_WRAPPERS
#include <stats.hpp>
#include <unsupported/Eigen/FFT>

namespace DB
{

Float64 approx_linear(const Float64 v, const std::vector<Float64> & x, const std::vector<Float64> & y)
{
    // Approximate y(v), given (x,y)[i], i = 0,..,n-1

    size_t i = 0, j = x.size() - 1;
    // handle out-of-domain points
    if (v < x[i] || v > x[j]) return 0.0;

    // find the correct interval by bisection
    while (i < j - 1) { // x[i] <= v <= x[j]
        size_t ij = (i+j) / 2;
        // i+1 <= ij <= j-1
        if (v < x[ij]) j = ij;
        else i = ij;
        // still i < j
    }
    // provably have i == j-1

    // interpolation

    if (v == x[j]) return y[j];
    if (v == x[i]) return y[i];

    return y[i] + (y[j] - y[i]) * ((v - x[i])/(x[j] - x[i]));
}

Float64 var(const std::vector<Float64> & x)
{
    Float64 tot = 0.0;
    for (auto v : x)
        tot += v;
    Float64 mean = tot / x.size();

    tot = 0.0;
    for (auto v : x)
        tot += (v - mean) * (v - mean);

    return tot / static_cast<Float64>(x.size() - 1);
}

Float64 quantile(std::vector<Float64> & x, const Float64 probs)
{
    const size_t n = x.size();
    const size_t np = 1;

    if (n == 0 || np == 0)
        return 0.0;

    Float64 index = 1.0 + (n - 1.0) * probs;
    Float64 lo = std::floor(index);
    Float64 hi = std::ceil(index);
    std::sort(x.begin(), x.end());
    Float64 qs = x[lo];
    Float64 h = index - lo;
    qs = (1 - h) * qs + h * x[hi];
    return qs;
}

Float64 nrd(std::vector<Float64> & x)
{
    Float64 r1 = quantile(x, 0.25);
    Float64 r2 = quantile(x, 0.75);
    Float64 h = (r2 - r1) / 1.34;
    Float64 p = std::pow(static_cast<Float64>(x.size()), -1.0/5.0);
    Float64 sd = std::sqrt(var(x));
    Float64 m = std::min(sd, h);
    return 1.06 * m * p;
}

std::vector<Float64> bin_dist(const std::vector<Float64> & sx, const Float64 sw, const Float64 slo, const Float64 shi, const size_t sn)
{
    std::vector<Float64> ans(2*sn);

    const size_t n = sn;
    Float64 xlo = slo, xhi = shi;
    auto &y = ans;

    int ixmin = 0, ixmax = n - 2;
    Float64 xdelta = (xhi - xlo) / static_cast<Float64>(n - 1);

    for (size_t i = 0; i < 2*n ; ++i)
        y[i] = 0;

    for (size_t i = 0; i < sx.size() ; ++i)
    {
        Float64 xpos = (sx[i] - xlo) / xdelta;
        int ix = static_cast<int>(std::floor(xpos));
        Float64 fx = xpos - static_cast<Float64>(ix);
        if (ixmin <= ix && ix <= ixmax)
        {
            y[ix] += (1 - fx) * sw;
            y[ix + 1] += fx * sw;
        }
        else if (ix == -1)
        {
            y[0] += fx * sw;
        }
        else if (ix == ixmax + 1)
        {
            y[ix] += (1 - fx) * sw;
        }
    }

    return ans;
}

void density(std::vector<Float64> & x, std::vector<Float64> & result_xs, std::vector<Float64> & result_ys)
{
    const size_t N = x.size();
    const Float64 weight = 1.0 / static_cast<Float64>(N);
    const Float64 bw = nrd(x);
    const size_t cut = 3;
    const Float64 from = (*std::min_element(x.begin(), x.end())) - cut * bw;
    const Float64 to = (*std::max_element(x.begin(), x.end())) + cut * bw;

    const Float64 lo = from - 4 * bw;
    const Float64 up = to + 4 * bw;

    const size_t n = 512;
    std::vector<Float64> y = bin_dist(x, weight, lo, up, n);

    std::vector<Float64> kords(n * 2);
    const Float64 kords_max = 2*(up-lo);
    const Float64 kords_inc = kords_max / (n * 2);
    for (size_t i = 0; i < kords.size(); ++i)
    {
        kords[i] = static_cast<Float64>(i) * kords_inc;
    }
    size_t j = 0;
    for (size_t i = n + 2; i < n * 2; ++i)
    {
        kords[i] = -kords[n - j];
        ++j;
    }

    std::vector<Float64> kords_dnorm = stats::dnorm(kords, 0, bw);

    // FFT
    typedef std::complex<Float64> C;

    std::vector<C> y_fft;
    {
        Eigen::FFT<Float64> fft;
        fft.fwd(y_fft, y);
    }

    std::vector<C> kords_fft;
    {
        Eigen::FFT<Float64> fft;
        fft.fwd(kords_fft, kords_dnorm);
        for (size_t i = 0; i < kords_fft.size(); ++i)
        {
            kords_fft[i] = std::conj(kords_fft[i]);
        }
    }

    std::vector<C> kords_mul(n * 2);
    for (size_t i = 0; i < kords_fft.size(); ++i)
    {
        kords_mul[i] = kords_fft[i] * y_fft[i % y_fft.size()];
    }

    std::vector<C> kords_inv(n * 2);
    {
        Eigen::FFT<Float64> fft;
        fft.inv(kords_inv, kords_mul);
    }

    std::vector<Float64> kords_n(n);
    for (size_t i = 0; i < n; ++i)
    {
        kords_n[i] = kords_inv[i].real();
    }

    // Approximate values for x-axis
    std::vector<Float64> xords(n);
    const Float64 xords_inc = (up - lo) / static_cast<Float64>(512);
    for (size_t i = 0; i < n; ++i)
    {
        xords[i] = lo + xords_inc * static_cast<Float64>(i);
    }

    result_xs.resize(n);
    const Float64 result_xs_inc = (to - from) / n;
    for (size_t i = 0; i < n; ++i)
    {
        result_xs[i] = from + i * result_xs_inc;
    }

    result_ys.resize(n);

    // Approximate values for y-axis
    for (size_t i = 0; i < result_xs.size(); ++i)
        result_ys[i] = approx_linear(result_xs[i], xords, kords_n);
}

}

#endif
