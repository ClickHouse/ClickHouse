#include <Client/BuzzHouse/Generator/ProbabilityGenerator.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <algorithm>
#include <random>

#include <base/defines.h>
#include <Common/randomSeed.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
}
}

namespace BuzzHouse
{

ProbabilityGenerator::ProbabilityGenerator(
    const ProbabilityStrategy ps, const uint64_t in_seed, const std::vector<ProbabilityBounds> & b, const String & desc)
    : nvalues(b.size())
    , strategy(ps)
    , bounds(b)
    , seed(in_seed ? in_seed : randomSeed())
    , generator(seed)
    , cdf(nvalues, 0.0)
    , enabled_values(nvalues, true)
    , description(desc)
{
    ensureAtLeastOneEnabled(enabled_values);
    probabilities = generateInitial();
    applyEnabledMaskAndRenorm(probabilities);
    buildCdf();
}

uint64_t ProbabilityGenerator::getSeed() const
{
    return seed;
}

ProbabilityStrategy ProbabilityGenerator::getStrategy() const
{
    return strategy;
}

const std::vector<double> & ProbabilityGenerator::getProbs() const
{
    return probabilities;
}

const std::vector<bool> & ProbabilityGenerator::getEnabledMask() const
{
    return enabled_values;
}

void ProbabilityGenerator::setEnabled(const std::vector<bool> & mask)
{
    ensureAtLeastOneEnabled(mask);
    enabled_values = mask;

    /// Zero out disabled and renormalize among enabled.
    applyEnabledMaskAndRenorm(probabilities);

    /// If you're using bounded realism / drifting, enforce bounds among enabled.
    if (strategy == ProbabilityStrategy::BoundedRealism || strategy == ProbabilityStrategy::Drifting)
        clampToBoundsAndRenormEnabled(probabilities, enabled_values);

    buildCdf();
}

void ProbabilityGenerator::setEnabled(const size_t i, const bool on)
{
    if (i >= nvalues)
        throw std::out_of_range("setEnabled index out of range");
    auto mask = enabled_values;
    mask[i] = on;
    setEnabled(mask);
}

size_t ProbabilityGenerator::nextOp(const bool tick)
{
    if (tick)
        this->tick();

    const double u = uniform_unit(generator);

    /// Binary search for the first bucket whose cdf is strictly greater than u.
    /// O(log N) per sample versus the previous O(N) linear scan. The CDF is built so the
    /// last enabled bucket has cdf == 1.0 (see buildCdf); disabled buckets keep the running
    /// sum unchanged so they cannot be picked unless u falls exactly on a tie, which is
    /// vanishingly unlikely with uniform_real_distribution.
    const auto it = std::upper_bound(cdf.begin(), cdf.end(), u);
    if (it == cdf.end())
        return lastEnabledIndex();
    const size_t idx = static_cast<size_t>(it - cdf.begin());
    /// Guard the FP-edge case where u rounds up onto a disabled trailing bucket whose CDF
    /// was forced to 1.0 prior to this fix.
    return enabled_values[idx] ? idx : lastEnabledIndex();
}

void ProbabilityGenerator::tick()
{
    ++ops_emitted;
    if (strategy == ProbabilityStrategy::Drifting && drift_every_n_ops > 0)
    {
        if (ops_emitted % drift_every_n_ops == 0)
        {
            applyDrift();
            buildCdf();
        }
    }
}

void ProbabilityGenerator::ensureAtLeastOneEnabled(const std::vector<bool> & mask)
{
    bool any = false;
    for (bool b : mask)
        any |= b;
    if (!any)
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "At least one option must be enabled");
}

size_t ProbabilityGenerator::lastEnabledIndex() const
{
    for (size_t i = nvalues; i-- > 0;)
        if (enabled_values[i])
            return i;
    /// Should be impossible due to invariant
    return 0;
}

std::vector<double> ProbabilityGenerator::generateInitial()
{
    switch (strategy)
    {
        case ProbabilityStrategy::Balanced: return genBalanced();
        case ProbabilityStrategy::BoundedRealism: return genBounded();
        case ProbabilityStrategy::Drifting: return genBounded(); /// start from bounded
    }
    /// Defensive against future enum additions; current enumerators all return above.
    throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown ProbabilityStrategy at {}", description);
}

std::vector<double> ProbabilityGenerator::genBalanced()
{
    /// Resample a few times to avoid extreme skew among enabled ops.
    for (int attempt = 0; attempt < std::max(1, balanced_resample_attempts); ++attempt)
    {
        std::vector<double> w(nvalues, 0.0);
        std::exponential_distribution<double> exp_dist(1.0);
        for (size_t i = 0; i < nvalues; ++i)
            w[i] = enabled_values[i] ? (exp_dist(generator) + kEpsilon) : 0.0;

        normalizeEnabledInPlace(w, enabled_values);

        const auto [mn, mx] = minmaxEnabled(w, enabled_values);
        const double ratio = (mn > 0.0) ? (mx / mn) : std::numeric_limits<double>::infinity();
        if (ratio <= balanced_max_ratio)
            return w;
    }

    /// fallback
    std::vector<double> w(nvalues, 0.0);
    std::exponential_distribution<double> exp_dist(1.0);
    for (size_t i = 0; i < nvalues; ++i)
        w[i] = enabled_values[i] ? (exp_dist(generator) + kEpsilon) : 0.0;

    normalizeEnabledInPlace(w, enabled_values);
    return w;
}

std::vector<double> ProbabilityGenerator::genBounded()
{
    std::vector<double> w(nvalues, 0.0);

    for (size_t i = 0; i < nvalues; ++i)
    {
        if (!enabled_values[i])
        {
            w[i] = 0.0;
            continue;
        }
        const auto b = bounds[i];
        if (b.min < 0.0 || b.max < 0.0 || b.min > b.max)
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Invalid bounds at {} index {}: min={}, max={}", description, i, b.min, b.max);
        std::uniform_real_distribution<double> unif(b.min, b.max);
        w[i] = unif(generator);
    }
    normalizeEnabledInPlace(w, enabled_values);
    clampToBoundsAndRenormEnabled(w, enabled_values);
    return w;
}

void ProbabilityGenerator::applyDrift()
{
    /// drift only enabled ops
    std::uniform_real_distribution<double> unif(-drift_strength, drift_strength);
    std::vector<double> w = probabilities;

    for (size_t i = 0; i < nvalues; ++i)
    {
        if (!enabled_values[i])
        {
            w[i] = 0.0;
            continue;
        }
        w[i] = w[i] * (1.0 + unif(generator));
        w[i] = std::max(w[i], 0.0);
    }

    normalizeEnabledInPlace(w, enabled_values);
    clampToBoundsAndRenormEnabled(w, enabled_values);
    probabilities = w;
}

void ProbabilityGenerator::buildCdf()
{
    double running = 0.0;

    for (size_t i = 0; i < nvalues; ++i)
    {
        running += probabilities[i];
        cdf[i] = running;
    }
    /// Pin the LAST ENABLED bucket's cdf to exactly 1.0 so floating-point drift in the
    /// running sum can never leave a sliver of probability that maps to a disabled trailing
    /// bucket. Previously this unconditionally set cdf[nvalues - 1] = 1.0, which gave the
    /// disabled last bucket the full upper tail of the unit interval.
    const size_t last_enabled = lastEnabledIndex();
    cdf[last_enabled] = 1.0;
    /// Carry that 1.0 forward over any disabled trailing buckets so upper_bound still
    /// terminates inside the cdf array for u close to 1.0.
    for (size_t i = last_enabled + 1; i < nvalues; ++i)
        cdf[i] = 1.0;
}

void ProbabilityGenerator::applyEnabledMaskAndRenorm(std::vector<double> & p) const
{
    chassert(p.size() == nvalues && enabled_values.size() == nvalues);
    for (size_t i = 0; i < nvalues; ++i)
        if (!enabled_values[i])
            p[i] = 0.0;
    normalizeEnabledInPlace(p, enabled_values);
}

void ProbabilityGenerator::normalizeEnabledInPlace(std::vector<double> & v, const std::vector<bool> & enabled) const
{
    double sum = 0.0;

    chassert(v.size() == nvalues && enabled.size() == nvalues);
    for (size_t i = 0; i < nvalues; ++i)
        if (enabled[i])
            sum += v[i];

    if (sum <= 0.0)
    {
        /// Put all mass on the first enabled op (guaranteed to exist).
        for (size_t i = 0; i < nvalues; ++i)
            v[i] = 0.0;
        for (size_t i = 0; i < nvalues; ++i)
        {
            if (enabled[i])
            {
                v[i] = 1.0;
                break;
            }
        }
        return;
    }

    for (size_t i = 0; i < nvalues; ++i)
        v[i] = enabled[i] ? (v[i] / sum) : 0.0;
}

std::pair<double, double> ProbabilityGenerator::minmaxEnabled(const std::vector<double> & v, const std::vector<bool> & enabled) const
{
    double mn = std::numeric_limits<double>::infinity();
    double mx = 0.0;

    chassert(v.size() == nvalues && enabled.size() == nvalues);
    for (size_t i = 0; i < nvalues; ++i)
    {
        if (!enabled[i])
            continue;
        mn = std::min(mn, v[i]);
        mx = std::max(mx, v[i]);
    }
    if (!std::isfinite(mn))
        mn = 0.0; /// should not happen
    return {mn, mx};
}

void ProbabilityGenerator::clampToBoundsAndRenormEnabled(std::vector<double> & p, const std::vector<bool> & enabled)
{
    /// Validate bounds feasibility for enabled subset
    double sum_min = 0.0;
    double sum_max = 0.0;

    chassert(p.size() == nvalues && bounds.size() == nvalues && enabled.size() == nvalues);
    for (size_t i = 0; i < nvalues; ++i)
    {
        if (!enabled[i])
            continue;
        sum_min += bounds[i].min;
        sum_max += bounds[i].max;
    }
    if (sum_min > 1.0 + kEpsilon)
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Inconsistent bounds for enabled subset at {} (cannot min sum to 1)", description);
    if (sum_max < 1.0 - kEpsilon)
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Inconsistent bounds for enabled subset at {} (cannot max sum to 1)", description);

    std::vector<bool> fixed(nvalues, false);
    for (int iter = 0; iter < kClampMaxIterations; ++iter)
    {
        for (size_t i = 0; i < nvalues; ++i)
        {
            if (!enabled[i])
            {
                p[i] = 0.0;
                fixed[i] = true;
                continue;
            }

            if (p[i] < bounds[i].min)
            {
                p[i] = bounds[i].min;
                fixed[i] = true;
            }
            else if (p[i] > bounds[i].max)
            {
                p[i] = bounds[i].max;
                fixed[i] = true;
            }
            else
                fixed[i] = false;
        }

        double used = 0.0;
        for (size_t i = 0; i < nvalues; ++i)
            used += p[i];
        double remaining = 1.0 - used;

        if (std::fabs(remaining) < kEpsilon)
        {
            /// tiny adjustment to first enabled op
            for (size_t i = 0; i < nvalues; ++i)
            {
                if (enabled[i])
                {
                    p[i] += remaining;
                    break;
                }
            }
            normalizeEnabledInPlace(p, enabled);
            return;
        }

        double free_mass = 0.0;
        for (size_t i = 0; i < nvalues; ++i)
            if (enabled[i] && !fixed[i])
                free_mass += p[i];

        if (free_mass <= 0.0)
        {
            /// No free vars left; nudge first enabled and exit
            for (size_t i = 0; i < nvalues; ++i)
                if (enabled[i])
                {
                    p[i] += remaining;
                    break;
                }
            normalizeEnabledInPlace(p, enabled);
            return;
        }

        for (size_t i = 0; i < nvalues; ++i)
            if (enabled[i] && !fixed[i])
                p[i] += remaining * (p[i] / free_mass);
    }

    normalizeEnabledInPlace(p, enabled);
}

}
