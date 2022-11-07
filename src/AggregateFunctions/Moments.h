#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <boost/math/distributions/students_t.hpp>
#include <boost/math/distributions/normal.hpp>
#include <cfloat>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}


/**
    Calculating univariate central moments
    Levels:
        level 2 (pop & samp): var, stddev
        level 3: skewness
        level 4: kurtosis
    References:
        https://en.wikipedia.org/wiki/Moment_(mathematics)
        https://en.wikipedia.org/wiki/Skewness
        https://en.wikipedia.org/wiki/Kurtosis
*/
template <typename T, size_t _level>
struct VarMoments
{
    T m[_level + 1]{};

    void add(T x)
    {
        ++m[0];
        m[1] += x;
        m[2] += x * x;
        if constexpr (_level >= 3) m[3] += x * x * x;
        if constexpr (_level >= 4) m[4] += x * x * x * x;
    }

    void merge(const VarMoments & rhs)
    {
        m[0] += rhs.m[0];
        m[1] += rhs.m[1];
        m[2] += rhs.m[2];
        if constexpr (_level >= 3) m[3] += rhs.m[3];
        if constexpr (_level >= 4) m[4] += rhs.m[4];
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    T getPopulation() const
    {
        if (m[0] == 0)
            return std::numeric_limits<T>::quiet_NaN();

        /// Due to numerical errors, the result can be slightly less than zero,
        /// but it should be impossible. Trim to zero.

        return std::max(T{}, (m[2] - m[1] * m[1] / m[0]) / m[0]);
    }

    T getSample() const
    {
        if (m[0] <= 1)
            return std::numeric_limits<T>::quiet_NaN();
        return std::max(T{}, (m[2] - m[1] * m[1] / m[0]) / (m[0] - 1));
    }

    T getMoment3() const
    {
        if (m[0] == 0)
            return std::numeric_limits<T>::quiet_NaN();
        // to avoid accuracy problem
        if (m[0] == 1)
            return 0;
        /// \[ \frac{1}{m_0} (m_3 - (3 * m_2 - \frac{2 * {m_1}^2}{m_0}) * \frac{m_1}{m_0});\]
        return (m[3]
            - (3 * m[2]
                - 2 * m[1] * m[1] / m[0]
            ) * m[1] / m[0]
        ) / m[0];
    }

    T getMoment4() const
    {
        if (m[0] == 0)
            return std::numeric_limits<T>::quiet_NaN();
        // to avoid accuracy problem
        if (m[0] == 1)
            return 0;
        /// \[ \frac{1}{m_0}(m_4 - (4 * m_3 - (6 * m_2 - \frac{3 * m_1^2}{m_0} ) \frac{m_1}{m_0})\frac{m_1}{m_0})\]
        return (m[4]
            - (4 * m[3]
                - (6 * m[2]
                    - 3 * m[1] * m[1] / m[0]
                ) * m[1] / m[0]
            ) * m[1] / m[0]
        ) / m[0];
    }
};

template <typename T, size_t _level>
class VarMomentsDecimal
{
public:
    using NativeType = typename T::NativeType;

    void add(NativeType x)
    {
        ++m0;
        getM(1) += x;

        NativeType tmp;
        bool overflow = common::mulOverflow(x, x, tmp) || common::addOverflow(getM(2), tmp, getM(2));
        if constexpr (_level >= 3)
            overflow = overflow || common::mulOverflow(tmp, x, tmp) || common::addOverflow(getM(3), tmp, getM(3));
        if constexpr (_level >= 4)
            overflow = overflow || common::mulOverflow(tmp, x, tmp) || common::addOverflow(getM(4), tmp, getM(4));

        if (overflow)
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }

    void merge(const VarMomentsDecimal & rhs)
    {
        m0 += rhs.m0;
        getM(1) += rhs.getM(1);

        bool overflow = common::addOverflow(getM(2), rhs.getM(2), getM(2));
        if constexpr (_level >= 3)
            overflow = overflow || common::addOverflow(getM(3), rhs.getM(3), getM(3));
        if constexpr (_level >= 4)
            overflow = overflow || common::addOverflow(getM(4), rhs.getM(4), getM(4));

        if (overflow)
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }

    void write(WriteBuffer & buf) const { writePODBinary(*this, buf); }
    void read(ReadBuffer & buf) { readPODBinary(*this, buf); }

    Float64 getPopulation(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(getM(1), getM(1), tmp) ||
            common::subOverflow(getM(2), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return std::max(Float64{}, DecimalUtils::convertTo<Float64>(T(tmp / m0), scale));
    }

    Float64 getSample(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::quiet_NaN();
        if (m0 == 1)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(getM(1), getM(1), tmp) ||
            common::subOverflow(getM(2), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return std::max(Float64{}, DecimalUtils::convertTo<Float64>(T(tmp / (m0 - 1)), scale));
    }

    Float64 getMoment3(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(2 * getM(1), getM(1), tmp) ||
            common::subOverflow(3 * getM(2), NativeType(tmp / m0), tmp) ||
            common::mulOverflow(tmp, getM(1), tmp) ||
            common::subOverflow(getM(3), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return DecimalUtils::convertTo<Float64>(T(tmp / m0), scale);
    }

    Float64 getMoment4(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(3 * getM(1), getM(1), tmp) ||
            common::subOverflow(6 * getM(2), NativeType(tmp / m0), tmp) ||
            common::mulOverflow(tmp, getM(1), tmp) ||
            common::subOverflow(4 * getM(3), NativeType(tmp / m0), tmp) ||
            common::mulOverflow(tmp, getM(1), tmp) ||
            common::subOverflow(getM(4), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return DecimalUtils::convertTo<Float64>(T(tmp / m0), scale);
    }

private:
    UInt64 m0{};
    NativeType m[_level]{};

    NativeType & getM(size_t i) { return m[i - 1]; }
    const NativeType & getM(size_t i) const { return m[i - 1]; }
};

/**
    Calculating multivariate central moments
    Levels:
        level 2 (pop & samp): covar
    References:
        https://en.wikipedia.org/wiki/Moment_(mathematics)
*/
template <typename T>
struct CovarMoments
{
    T m0{};
    T x1{};
    T y1{};
    T xy{};

    void add(T x, T y)
    {
        ++m0;
        x1 += x;
        y1 += y;
        xy += x * y;
    }

    void merge(const CovarMoments & rhs)
    {
        m0 += rhs.m0;
        x1 += rhs.x1;
        y1 += rhs.y1;
        xy += rhs.xy;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    T NO_SANITIZE_UNDEFINED getPopulation() const
    {
        return (xy - x1 * y1 / m0) / m0;
    }

    T NO_SANITIZE_UNDEFINED getSample() const
    {
        if (m0 == 0)
            return std::numeric_limits<T>::quiet_NaN();
        return (xy - x1 * y1 / m0) / (m0 - 1);
    }
};

template <typename T>
struct CorrMoments
{
    T m0{};
    T x1{};
    T y1{};
    T xy{};
    T x2{};
    T y2{};

    void add(T x, T y)
    {
        ++m0;
        x1 += x;
        y1 += y;
        xy += x * y;
        x2 += x * x;
        y2 += y * y;
    }

    void merge(const CorrMoments & rhs)
    {
        m0 += rhs.m0;
        x1 += rhs.x1;
        y1 += rhs.y1;
        xy += rhs.xy;
        x2 += rhs.x2;
        y2 += rhs.y2;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    T NO_SANITIZE_UNDEFINED get() const
    {
        return (m0 * xy - x1 * y1) / sqrt((m0 * x2 - x1 * x1) * (m0 * y2 - y1 * y1));
    }
};

/// Data for calculation of Student and Welch T-Tests.
template <typename T>
struct TTestMoments
{
    T nx{};
    T ny{};
    T x1{};
    T y1{};
    T x2{};
    T y2{};

    void addX(T value)
    {
        ++nx;
        x1 += value;
        x2 += value * value;
    }

    void addY(T value)
    {
        ++ny;
        y1 += value;
        y2 += value * value;
    }

    void merge(const TTestMoments & rhs)
    {
        nx += rhs.nx;
        ny += rhs.ny;
        x1 += rhs.x1;
        y1 += rhs.y1;
        x2 += rhs.x2;
        y2 += rhs.y2;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    Float64 getMeanX() const
    {
        return x1 / nx;
    }

    Float64 getMeanY() const
    {
        return y1 / ny;
    }

    Float64 getStandardError() const
    {
        /// The original formulae looks like  \frac{1}{size_x - 1} \sum_{i = 1}^{size_x}{(x_i - \bar{x}) ^ 2}
        /// But we made some mathematical transformations not to store original sequences.
        /// Also we dropped sqrt, because later it will be squared later.
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();

        Float64 sx2 = (x2 + nx * mean_x * mean_x - 2 * mean_x * x1) / (nx - 1);
        Float64 sy2 = (y2 + ny * mean_y * mean_y - 2 * mean_y * y1) / (ny - 1);

        return sqrt(sx2 / nx + sy2 / ny);
    }

    std::pair<Float64, Float64> getConfidenceIntervals(Float64 confidence_level, Float64 degrees_of_freedom) const
    {
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();
        Float64 se = getStandardError();

        boost::math::students_t dist(degrees_of_freedom);
        Float64 t = boost::math::quantile(boost::math::complement(dist, (1.0 - confidence_level) / 2.0));
        Float64 mean_diff = mean_x - mean_y;
        Float64 ci_low = mean_diff - t * se;
        Float64 ci_high = mean_diff + t * se;

        return {ci_low, ci_high};
    }

    bool isEssentiallyConstant() const
    {
        return getStandardError() < 10 * DBL_EPSILON * std::max(std::abs(getMeanX()), std::abs(getMeanY()));
    }
};

template <typename T>
struct ZTestMoments
{
    T nx{};
    T ny{};
    T x1{};
    T y1{};

    void addX(T value)
    {
        ++nx;
        x1 += value;
    }

    void addY(T value)
    {
        ++ny;
        y1 += value;
    }

    void merge(const ZTestMoments & rhs)
    {
        nx += rhs.nx;
        ny += rhs.ny;
        x1 += rhs.x1;
        y1 += rhs.y1;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    Float64 getMeanX() const
    {
        return x1 / nx;
    }

    Float64 getMeanY() const
    {
        return y1 / ny;
    }

    Float64 getStandardError(Float64 pop_var_x, Float64 pop_var_y) const
    {
        /// \sqrt{\frac{\sigma_{1}^{2}}{n_{1}} + \frac{\sigma_{2}^{2}}{n_{2}}}
        return std::sqrt(pop_var_x / nx + pop_var_y / ny);
    }

    std::pair<Float64, Float64> getConfidenceIntervals(Float64 pop_var_x, Float64 pop_var_y, Float64 confidence_level) const
    {
        /// (\bar{x_{1}} - \bar{x_{2}}) \pm zscore \times \sqrt{\frac{\sigma_{1}^{2}}{n_{1}} + \frac{\sigma_{2}^{2}}{n_{2}}}
        Float64 mean_x = getMeanX();
        Float64 mean_y = getMeanY();

        Float64 z = boost::math::quantile(boost::math::complement(
            boost::math::normal(0.0f, 1.0f), (1.0f - confidence_level) / 2.0f));
        Float64 se = getStandardError(pop_var_x, pop_var_y);
        Float64 ci_low = (mean_x - mean_y) - z * se;
        Float64 ci_high = (mean_x - mean_y) + z * se;

        return {ci_low, ci_high};
    }
};

}
