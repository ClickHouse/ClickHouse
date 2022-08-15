#pragma once

#include <numeric>
#include <algorithm>
#include <utility>

#include <base/sort.h>

#include <Common/ArenaAllocator.h>
#include <Common/logger_useful.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <boost/math/distributions/normal.hpp>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Because ranks are adjusted, we have to store each of them in Float type.
using RanksArray = std::vector<Float64>;

template <typename Values>
std::pair<RanksArray, Float64> computeRanksAndTieCorrection(const Values & values)
{
    const size_t size = values.size();
    /// Save initial positions, than sort indices according to the values.
    std::vector<size_t> indexes(size);
    std::iota(indexes.begin(), indexes.end(), 0);
    std::sort(indexes.begin(), indexes.end(),
        [&] (size_t lhs, size_t rhs) { return values[lhs] < values[rhs]; });

    size_t left = 0;
    Float64 tie_numenator = 0;
    RanksArray out(size);
    while (left < size)
    {
        size_t right = left;
        while (right < size && values[indexes[left]] == values[indexes[right]])
            ++right;
        auto adjusted = (left + right + 1.) / 2.;
        auto count_equal = right - left;

        /// Scipy implementation throws exception in this case too.
        if (count_equal == size)
            throw Exception("All numbers in both samples are identical", ErrorCodes::BAD_ARGUMENTS);

        tie_numenator += std::pow(count_equal, 3) - count_equal;
        for (size_t iter = left; iter < right; ++iter)
            out[indexes[iter]] = adjusted;
        left = right;
    }
    return {out, 1 - (tie_numenator / (std::pow(size, 3) - size))};
}


template <typename X, typename Y>
struct StatisticalSample
{
    using AllocatorXSample = MixedAlignedArenaAllocator<alignof(X), 4096>;
    using SampleX = PODArray<X, 32, AllocatorXSample>;

    using AllocatorYSample = MixedAlignedArenaAllocator<alignof(Y), 4096>;
    using SampleY = PODArray<Y, 32, AllocatorYSample>;

    SampleX x{};
    SampleY y{};
    size_t size_x{0};
    size_t size_y{0};

    void addX(X value, Arena * arena)
    {
        if (isNaN(value))
            return;

        ++size_x;
        x.push_back(value, arena);
    }

    void addY(Y value, Arena * arena)
    {
        if (isNaN(value))
            return;

        ++size_y;
        y.push_back(value, arena);
    }

    void merge(const StatisticalSample & rhs, Arena * arena)
    {
        size_x += rhs.size_x;
        size_y += rhs.size_y;
        x.insert(rhs.x.begin(), rhs.x.end(), arena);
        y.insert(rhs.y.begin(), rhs.y.end(), arena);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size_x, buf);
        writeVarUInt(size_y, buf);
        buf.write(reinterpret_cast<const char *>(x.data()), size_x * sizeof(x[0]));
        buf.write(reinterpret_cast<const char *>(y.data()), size_y * sizeof(y[0]));
    }

    void read(ReadBuffer & buf, Arena * arena)
    {
        readVarUInt(size_x, buf);
        readVarUInt(size_y, buf);
        x.resize(size_x, arena);
        y.resize(size_y, arena);
        buf.read(reinterpret_cast<char *>(x.data()), size_x * sizeof(x[0]));
        buf.read(reinterpret_cast<char *>(y.data()), size_y * sizeof(y[0]));
    }
};

template <typename T>
struct ShapiroWilkTestSample {
    using SampleAllocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Sample = PODArray<T, 32, SampleAllocator>;

    Sample data{};
    size_t size{0};

    void add(T value, Arena * arena)
    {
        ++size;
        data.push_back(value, arena);
    }

    void merge(const ShapiroWilkTestSample & rhs, Arena * arena)
    {
        size += rhs.size;
        data.insert(rhs.data.begin(), rhs.data.end(), arena);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size , buf);
        buf.write(reinterpret_cast<const char *>(data.data()), size * sizeof(data[0]));
    }

    void read(ReadBuffer & buf, Arena * arena)
    {
        readVarUInt(size, buf);
        data.resize(size, arena);
        buf.read(reinterpret_cast<char *>(data.data()), size * sizeof(data[0]));
    }

    double getWStatistic()
    {
        std::sort(data.begin(), data.end());
        double sum_x = 0, sum_x2 = 0, sum_m2 = 0, sum_ax = 0;
        assert(size >= 4);
        double m_l = 0, m_bl = 0;

        for (size_t i = 0; i < size; i++) {
            double c = (i + 0.625) / (size + 0.25);
            double m = -boost::math::quantile(boost::math::complement(boost::math::normal(), c));
            sum_m2 += m * m;
            if (i == size - 1) {
                m_l = m;
            }
            if (i == size - 2) {
                m_bl = m;
            }
        }

        double a_l = 0, a_bl = 0;
        double u = 1 / sqrt(size);
        a_l = -2.706056 * pow(u, 5) + 4.434685 * pow(u, 4) - 2.071190 * pow(u, 3) - 0.147981 * u * u + 0.221157 * u + m_l / sqrt(sum_m2);
        a_bl = -3.582633 * pow(u, 5) + 5.682633 * pow(u, 4) - 1.752461 * pow(u, 3) - 0.293762 * u * u + 0.042981 * u + m_bl / sqrt(sum_m2);
        double a_1 = -a_l;
        double a_2 = -a_bl;
        double eps = (sum_m2 - 2 * m_l * m_l - 2 * m_bl *  m_bl) / (1 - 2 * a_l * a_l - 2 * a_bl *  a_bl);
        for (size_t i = 0; i < size; i++) {
            double x = data[i];
            sum_x += x;
            sum_x2 += x * x;
            double c = (i + 0.625) / (size + 0.25);
            double m = -boost::math::quantile(boost::math::complement(boost::math::normal(), c));
            double a = m / sqrt(eps);
            if (i == 0) {
                a = a_1;
            }
            if (i == 1) {
                a = a_2;
            }
            if (i == size-1) {
                a = a_l;
            }
            if (i == size-2) {
                a = a_bl;
            }
            LOG_DEBUG(&Poco::Logger::get("MemoryTracker"), "COEF MI {} AI {} I {}", m, a, i);
            sum_ax += a * x;
        }
        double avg = sum_x / size;
        double w_stat = sum_ax * sum_ax / (sum_x2 - 2 * avg * sum_x + size * avg * avg);
        return w_stat;
    }

    double getPValue(double w_stat)
    {
        double t = log(size);
        double w = log(1 - w_stat);
        double mu = -1.5861 - 0.31082 * t - 0.083751 * t * t + 0.0038915 * t * t * t;
        double sigma = exp(-0.4803 - 0.082676 * t + 0.0030302 * t * t);
        double p_value = 1 - boost::math::cdf(boost::math::normal(), (w - mu) / sigma);
        return p_value;
    }

};

}
