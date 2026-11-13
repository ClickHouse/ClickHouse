// Dump of https://github.com/ankane/stl-cpp/blob/3b1b3a3e9335cda26c8b0797d8b8d24ac8e350ad/include/stl.hpp.
// Added to ClickHouse source code and not referenced as a submodule because its easier maintain and modify/customize.

/*!
 * STL C++ v0.1.3
 * https://github.com/ankane/stl-cpp
 * Unlicense OR MIT License
 *
 * Ported from https://www.netlib.org/a/stl
 *
 * Cleveland, R. B., Cleveland, W. S., McRae, J. E., & Terpenning, I. (1990).
 * STL: A Seasonal-Trend Decomposition Procedure Based on Loess.
 * Journal of Official Statistics, 6(1), 3-33.
 */

#pragma once

#include <algorithm>
#include <cmath>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <vector>

namespace stl {

inline bool est(const float* y, size_t n, size_t len, int ideg, float xs, float* ys, size_t nleft, size_t nright, float* w, bool userw, const float* rw) {
    auto range = ((float) n) - 1.0;
    auto h = std::max(xs - ((float) nleft), ((float) nright) - xs);

    if (len > n) {
        h += (float) ((len - n) / 2); /// NOLINT(bugprone-integer-division)
    }

    auto h9 = 0.999 * h;
    auto h1 = 0.001 * h;

    // compute weights
    auto a = 0.0;
    for (auto j = nleft; j <= nright; j++) {
        w[j - 1] = 0.0;
        auto r = fabs(((float) j) - xs);
        if (r <= h9) {
            if (r <= h1) {
                w[j - 1] = 1.0;
            } else {
                w[j - 1] = pow(1.0 - pow(r / h, 3), 3);
            }
            if (userw) {
                w[j - 1] *= rw[j - 1];
            }
            a += w[j - 1];
        }
    }

    if (a <= 0.0) {
        return false;
    } // weighted least squares
    for (auto j = nleft; j <= nright; j++)
    { // make sum of w(j) == 1
        w[j - 1] /= a;
    }

    if (h > 0.0 && ideg > 0)
    { // use linear fit
        auto a = 0.0;
        for (auto j = nleft; j <= nright; j++)
        { // weighted center of x values
            a += w[j - 1] * ((float)j);
        }
        auto b = xs - a;
        auto c = 0.0;
        for (auto j = nleft; j <= nright; j++)
        {
            c += w[j - 1] * pow(((float)j) - a, 2);
        }
        if (sqrt(c) > 0.001 * range)
        {
            b /= c;

            // points are spread out enough to compute slope
            for (auto j = nleft; j <= nright; j++)
            {
                w[j - 1] *= b * (((float)j) - a) + 1.0;
            }
        }
    }

    *ys = 0.0;
    for (auto j = nleft; j <= nright; j++)
    {
        *ys += w[j - 1] * y[j - 1];
    }

    return true;
}

inline void ess(const float* y, size_t n, size_t len, int ideg, size_t njump, bool userw, const float* rw, float* ys, float* res) {
    if (n < 2) {
        ys[0] = y[0];
        return;
    }

    size_t nleft = 0;
    size_t nright = 0;

    auto newnj = std::min(njump, n - 1);
    if (len >= n) {
        nleft = 1;
        nright = n;
        for (size_t i = 1; i <= n; i += newnj) {
            auto ok = est(y, n, len, ideg, (float) i, &ys[i - 1], nleft, nright, res, userw, rw);
            if (!ok) {
                ys[i - 1] = y[i - 1];
            }
        }
    } else if (newnj == 1) { // newnj equal to one, len less than n
        auto nsh = (len + 1) / 2;
        nleft = 1;
        nright = len;
        for (size_t i = 1; i <= n; i++) { // fitted value at i
            if (i > nsh && nright != n) {
                nleft += 1;
                nright += 1;
            }
            auto ok = est(y, n, len, ideg, (float) i, &ys[i - 1], nleft, nright, res, userw, rw);
            if (!ok) {
                ys[i - 1] = y[i - 1];
            }
        }
    } else { // newnj greater than one, len less than n
        auto nsh = (len + 1) / 2;
        for (size_t i = 1; i <= n; i += newnj) { // fitted value at i
            if (i < nsh) {
                nleft = 1;
                nright = len;
            } else if (i >= n - nsh + 1) {
                nleft = n - len + 1;
                nright = n;
            } else {
                nleft = i - nsh + 1;
                nright = len + i - nsh;
            }
            auto ok = est(y, n, len, ideg, (float) i, &ys[i - 1], nleft, nright, res, userw, rw);
            if (!ok) {
                ys[i - 1] = y[i - 1];
            }
        }
    }

    if (newnj != 1) {
        for (size_t i = 1; i <= n - newnj; i += newnj) {
            auto delta = (ys[i + newnj - 1] - ys[i - 1]) / ((float) newnj);
            for (auto j = i + 1; j <= i + newnj - 1; j++) {
                ys[j - 1] = ys[i - 1] + delta * ((float) (j - i));
            }
        }
        auto k = ((n - 1) / newnj) * newnj + 1;
        if (k != n) {
            auto ok = est(y, n, len, ideg, (float) n, &ys[n - 1], nleft, nright, res, userw, rw);
            if (!ok) {
                ys[n - 1] = y[n - 1];
            }
            if (k != n - 1) {
                auto delta = (ys[n - 1] - ys[k - 1]) / ((float) (n - k));
                for (auto j = k + 1; j <= n - 1; j++) {
                    ys[j - 1] = ys[k - 1] + delta * ((float) (j - k));
                }
            }
        }
    }
}

inline void ma(const float* x, size_t n, size_t len, float* ave) {
    auto newn = n - len + 1;
    auto flen = (float) len;
    auto v = 0.0;

    // get the first average
    for (size_t i = 0; i < len; i++) {
        v += x[i];
    }

    ave[0] = v / flen;
    if (newn > 1) {
        auto k = len;
        auto m = 0;
        for (size_t j = 1; j < newn; j++) {
            // window down the array
            v = v - x[m] + x[k];
            ave[j] = v / flen;
            k += 1;
            m += 1;
        }
    }
}

inline void fts(const float* x, size_t n, size_t np, float* trend, float* work) {
    ma(x, n, np, trend);
    ma(trend, n - np + 1, np, work);
    ma(work, n - 2 * np + 2, 3, trend);
}

inline void rwts(const float* y, size_t n, const float* fit, float* rw) {
    for (size_t i = 0; i < n; i++) {
        rw[i] = fabs(y[i] - fit[i]);
    }

    auto mid1 = (n - 1) / 2;
    auto mid2 = n / 2;

    // sort
    std::sort(rw, rw + n);

    auto cmad = 3.0 * (rw[mid1] + rw[mid2]); // 6 * median abs resid
    auto c9 = 0.999 * cmad;
    auto c1 = 0.001 * cmad;

    for (size_t i = 0; i < n; i++) {
        auto r = fabs(y[i] - fit[i]);
        if (r <= c1) {
            rw[i] = 1.0;
        } else if (r <= c9) {
            rw[i] = pow(1.0 - pow(r / cmad, 2), 2);
        } else {
            rw[i] = 0.0;
        }
    }
}

inline void ss(const float* y, size_t n, size_t np, size_t ns, int isdeg, size_t nsjump, bool userw, float* rw, float* season, float* work1, float* work2, float* work3, float* work4) { /// NOLINT(readability-non-const-parameter)
    for (size_t j = 1; j <= np; j++) {
        size_t k = (n - j) / np + 1;

        for (size_t i = 1; i <= k; i++) {
            work1[i - 1] = y[(i - 1) * np + j - 1];
        }
        if (userw) {
            for (size_t i = 1; i <= k; i++) {
                work3[i - 1] = rw[(i - 1) * np + j - 1];
            }
        }
        ess(work1, k, ns, isdeg, nsjump, userw, work3, work2 + 1, work4);
        auto xs = 0.0;
        auto nright = std::min(ns, k);
        auto ok = est(work1, k, ns, isdeg, xs, &work2[0], 1, nright, work4, userw, work3);
        if (!ok) {
            work2[0] = work2[1];
        }
        xs = k + 1;
        size_t nleft = std::max(1, (int) k - (int) ns + 1);
        ok = est(work1, k, ns, isdeg, xs, &work2[k + 1], nleft, k, work4, userw, work3);
        if (!ok) {
            work2[k + 1] = work2[k];
        }
        for (size_t m = 1; m <= k + 2; m++) {
            season[(m - 1) * np + j - 1] = work2[m - 1];
        }
    }
}

inline void onestp(const float* y, size_t n, size_t np, size_t ns, size_t nt, size_t nl, int isdeg, int itdeg, int ildeg, size_t nsjump, size_t ntjump, size_t nljump, size_t ni, bool userw, float* rw, float* season, float* trend, float* work1, float* work2, float* work3, float* work4, float* work5) {
    for (size_t j = 0; j < ni; j++) {
        for (size_t i = 0; i < n; i++) {
            work1[i] = y[i] - trend[i];
        }

        ss(work1, n, np, ns, isdeg, nsjump, userw, rw, work2, work3, work4, work5, season);
        fts(work2, n + 2 * np, np, work3, work1);
        ess(work3, n, nl, ildeg, nljump, false, work4, work1, work5);
        for (size_t i = 0; i < n; i++) {
            season[i] = work2[np + i] - work1[i];
        }
        for (size_t i = 0; i < n; i++) {
            work1[i] = y[i] - season[i];
        }
        ess(work1, n, nt, itdeg, ntjump, userw, rw, trend, work3);
    }
}

inline void stl(const float* y, size_t n, size_t np, size_t ns, size_t nt, size_t nl, int isdeg, int itdeg, int ildeg, size_t nsjump, size_t ntjump, size_t nljump, size_t ni, size_t no, float* rw, float* season, float* trend) {
    if (ns < 3) {
        throw std::invalid_argument("seasonal_length must be at least 3");
    }
    if (nt < 3) {
        throw std::invalid_argument("trend_length must be at least 3");
    }
    if (nl < 3) {
        throw std::invalid_argument("low_pass_length must be at least 3");
    }
    if (np < 2) {
        throw std::invalid_argument("period must be at least 2");
    }

    if (isdeg != 0 && isdeg != 1) {
        throw std::invalid_argument("seasonal_degree must be 0 or 1");
    }
    if (itdeg != 0 && itdeg != 1) {
        throw std::invalid_argument("trend_degree must be 0 or 1");
    }
    if (ildeg != 0 && ildeg != 1) {
        throw std::invalid_argument("low_pass_degree must be 0 or 1");
    }

    if (ns % 2 != 1) {
        throw std::invalid_argument("seasonal_length must be odd");
    }
    if (nt % 2 != 1) {
        throw std::invalid_argument("trend_length must be odd");
    }
    if (nl % 2 != 1) {
        throw std::invalid_argument("low_pass_length must be odd");
    }

    auto work1 = std::vector<float>(n + 2 * np);
    auto work2 = std::vector<float>(n + 2 * np);
    auto work3 = std::vector<float>(n + 2 * np);
    auto work4 = std::vector<float>(n + 2 * np);
    auto work5 = std::vector<float>(n + 2 * np);

    auto userw = false;
    size_t k = 0;

    while (true) {
        onestp(y, n, np, ns, nt, nl, isdeg, itdeg, ildeg, nsjump, ntjump, nljump, ni, userw, rw, season, trend, work1.data(), work2.data(), work3.data(), work4.data(), work5.data());
        k += 1;
        if (k > no) {
            break;
        }
        for (size_t i = 0; i < n; i++) {
            work1[i] = trend[i] + season[i];
        }
        rwts(y, n, work1.data(), rw);
        userw = true;
    }

    if (no <= 0) {
        for (size_t i = 0; i < n; i++) {
            rw[i] = 1.0;
        }
    }
}

inline float var(const std::vector<float>& series) {
    auto mean = std::accumulate(series.begin(), series.end(), 0.0) / series.size();
    std::vector<float> tmp;
    tmp.reserve(series.size());
    for (auto v : series) {
        tmp.push_back(pow(v - mean, 2));
    }
    return std::accumulate(tmp.begin(), tmp.end(), 0.0) / (series.size() - 1);
}

inline float strength(const std::vector<float>& component, const std::vector<float>& remainder) {
    std::vector<float> sr;
    sr.reserve(remainder.size());
    for (size_t i = 0; i < remainder.size(); i++) {
        sr.push_back(component[i] + remainder[i]);
    }
    return std::max(0.0, 1.0 - var(remainder) / var(sr));
}

class StlResult {
public:
    std::vector<float> seasonal;
    std::vector<float> trend;
    std::vector<float> remainder;
    std::vector<float> weights;

    float seasonal_strength() const {
        return strength(seasonal, remainder);
    }

    float trend_strength() const {
        return strength(trend, remainder);
    }
};

class StlParams {
    std::optional<size_t> ns_ = std::nullopt;
    std::optional<size_t> nt_ = std::nullopt;
    std::optional<size_t> nl_ = std::nullopt;
    int isdeg_ = 0;
    int itdeg_ = 1;
    std::optional<int> ildeg_ = std::nullopt;
    std::optional<size_t> nsjump_ = std::nullopt;
    std::optional<size_t> ntjump_ = std::nullopt;
    std::optional<size_t> nljump_ = std::nullopt;
    std::optional<size_t> ni_ = std::nullopt;
    std::optional<size_t> no_ = std::nullopt;
    bool robust_ = false;

public:
    StlParams seasonal_length(size_t ns) {
        this->ns_ = ns;
        return *this;
    }

    StlParams trend_length(size_t nt) {
        this->nt_ = nt;
        return *this;
    }

    StlParams low_pass_length(size_t nl) {
        this->nl_ = nl;
        return *this;
    }

    StlParams seasonal_degree(int isdeg) {
        this->isdeg_ = isdeg;
        return *this;
    }

    StlParams trend_degree(int itdeg) {
        this->itdeg_ = itdeg;
        return *this;
    }

    StlParams low_pass_degree(int ildeg) {
        this->ildeg_ = ildeg;
        return *this;
    }

    StlParams seasonal_jump(size_t nsjump) {
        this->nsjump_ = nsjump;
        return *this;
    }

    StlParams trend_jump(size_t ntjump) {
        this->ntjump_ = ntjump;
        return *this;
    }

    StlParams low_pass_jump(size_t nljump) {
        this->nljump_ = nljump;
        return *this;
    }

    StlParams inner_loops(bool ni) {
        this->ni_ = ni;
        return *this;
    }

    StlParams outer_loops(bool no) {
        this->no_ = no;
        return *this;
    }

    StlParams robust(bool robust) {
        this->robust_ = robust;
        return *this;
    }

    StlResult fit(const float* y, size_t n, size_t np);
    StlResult fit(const std::vector<float>& y, size_t np);
};

inline StlParams params() {
    return StlParams();
}

inline StlResult StlParams::fit(const float* y, size_t n, size_t np) {
    if (n < 2 * np) {
        throw std::invalid_argument("series has less than two periods");
    }

    auto ns = this->ns_.value_or(np);

    auto isdeg = this->isdeg_;
    auto itdeg = this->itdeg_;

    auto res = StlResult {
        std::vector<float>(n),
        std::vector<float>(n),
        std::vector<float>(),
        std::vector<float>(n)
    };

    auto ildeg = this->ildeg_.value_or(itdeg);
    auto newns = std::max(ns, (size_t) 3);
    if (newns % 2 == 0) {
        newns += 1;
    }

    auto newnp = std::max(np, (size_t) 2);
    auto nt = (size_t) ceil((1.5 * newnp) / (1.0 - 1.5 / (float) newns));
    nt = this->nt_.value_or(nt);
    nt = std::max(nt, (size_t) 3);
    if (nt % 2 == 0) {
        nt += 1;
    }

    auto nl = this->nl_.value_or(newnp);
    if (nl % 2 == 0 && !this->nl_.has_value()) {
        nl += 1;
    }

    auto ni = this->ni_.value_or(this->robust_ ? 1 : 2);
    auto no = this->no_.value_or(this->robust_ ? 15 : 0);

    auto nsjump = this->nsjump_.value_or((size_t) ceil(((float) newns) / 10.0));
    auto ntjump = this->ntjump_.value_or((size_t) ceil(((float) nt) / 10.0));
    auto nljump = this->nljump_.value_or((size_t) ceil(((float) nl) / 10.0));

    stl(y, n, newnp, newns, nt, nl, isdeg, itdeg, ildeg, nsjump, ntjump, nljump, ni, no, res.weights.data(), res.seasonal.data(), res.trend.data());

    res.remainder.reserve(n);
    for (size_t i = 0; i < n; i++) {
        res.remainder.push_back(y[i] - res.seasonal[i] - res.trend[i]);
    }

    return res;
}

inline StlResult StlParams::fit(const std::vector<float>& y, size_t np) {
    return StlParams::fit(y.data(), y.size(), np);
}

}
