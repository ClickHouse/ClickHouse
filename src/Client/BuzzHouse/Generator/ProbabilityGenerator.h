#pragma once

#include <algorithm>
#include <array>
#include <optional>
#include <random>
#include <stdexcept>
#include <vector>

#include <cassert>

namespace BuzzHouse
{

enum class ProbabilityStrategy
{
    Balanced = 0,
    BoundedRealism,
    Drifting
};

struct ProbabilityBounds
{
    double min = 0.0;
    double max = 1.0;
};

struct ProbabilityConfig
{
    ProbabilityStrategy strategy = ProbabilityStrategy::BoundedRealism;
    uint64_t seed;

    /// Balanced skew control
    double balanced_max_ratio = 6.0;
    int balanced_resample_attempts = 20;

    std::vector<ProbabilityBounds> bounds{}; /// used by Bounded + Drifting

    /// Drifting controls
    uint64_t drift_every_n_ops = 500;
    double drift_strength = 0.10; /// ±10%

    /// Runtime enablement default (all enabled)
    std::vector<bool> enabled = [this]
    {
        std::vector<bool> v(bounds.size(), true);
        return v;
    }();

    ProbabilityConfig(const ProbabilityStrategy & ps, const uint64_t & s, const std::vector<ProbabilityBounds> & b_)
        : strategy(ps)
        , seed(s)
        , bounds(b_)
    {
    }
};

class ProbabilityGeneratorT
{
public:
    const size_t nvalues;

    explicit ProbabilityGeneratorT(ProbabilityConfig _cfg)
        : nvalues(_cfg.bounds.size())
        , cfg(std::move(_cfg))
        , seed_used(cfg.seed)
        , cdf(nvalues, 0.0)
        , enabled_values(cfg.enabled)
    {
        rng.seed(seed_used);
        ensureAtLeastOneEnabled(enabled_values);
        probabilites = generateInitial();
        applyEnabledMaskAndRenorm(probabilites);
        buildCdf();
    }

    uint64_t seedUsed() const { return seed_used; }
    ProbabilityStrategy strategy() const { return cfg.strategy; }
    const std::vector<double> & probs() const { return probabilites; }
    const std::vector<bool> & enabled() const { return enabled_values; }

    /// Change enable mask at runtime. Preserves current distribution shape as much as possible.
    void setEnabled(const std::vector<bool> & mask)
    {
        ensureAtLeastOneEnabled(mask);
        enabled_values = mask;

        /// Zero out disabled and renormalize among enabled.
        applyEnabledMaskAndRenorm(probabilites);

        /// If you're using bounded realism / drifting, enforce bounds among enabled.
        if (cfg.strategy == ProbabilityStrategy::BoundedRealism || cfg.strategy == ProbabilityStrategy::Drifting)
            clampToBoundsAndRenormEnabled(probabilites, cfg.bounds, enabled_values);

        buildCdf();
    }

    void setEnabled(size_t i, bool on)
    {
        if (i >= nvalues)
            throw std::out_of_range("setEnabled index out of range");
        auto mask = enabled_values;
        mask[i] = on;
        setEnabled(mask);
    }

    /// Sample next op (ignores disabled ops automatically)
    size_t nextOp(bool tick = true)
    {
        if (tick)
            this->tick();

        std::uniform_real_distribution<double> unif01(0.0, 1.0);
        const double u = unif01(rng);

        for (size_t i = 0; i < nvalues; ++i)
            if (u < cdf[i])
                return i;

        /// FP edge
        return lastEnabledEnum();
    }

    void tick()
    {
        ++ops_emitted;
        if (cfg.strategy == ProbabilityStrategy::Drifting && cfg.drift_every_n_ops > 0)
        {
            if (ops_emitted % cfg.drift_every_n_ops == 0)
            {
                applyDrift();
                buildCdf();
            }
        }
    }

private:
    ProbabilityConfig cfg;
    std::mt19937_64 rng;
    uint64_t seed_used;

    std::vector<double> cdf;
    std::vector<bool> enabled_values;
    std::vector<double> probabilites;
    uint64_t ops_emitted = 0;

    static void ensureAtLeastOneEnabled(const std::vector<bool> & mask)
    {
        bool any = false;
        for (bool b : mask)
            any |= b;
        if (!any)
            throw std::runtime_error("At least one option must be enabled");
    }

    size_t lastEnabledEnum() const
    {
        for (size_t i = nvalues; i-- > 0;)
            if (enabled_values[i])
                return i;
        /// Should be impossible due to invariant
        return 0;
    }

    std::vector<double> generateInitial()
    {
        switch (cfg.strategy)
        {
            case ProbabilityStrategy::Balanced:
                return genBalanced();
            case ProbabilityStrategy::BoundedRealism:
                return genBounded();
            case ProbabilityStrategy::Drifting:
                return genBounded(); /// start from bounded
        }
    }

    std::vector<double> genBalanced()
    {
        /// Resample a few times to avoid extreme skew among enabled ops.
        for (int attempt = 0; attempt < std::max(1, cfg.balanced_resample_attempts); ++attempt)
        {
            std::vector<double> w(nvalues, 0.0);
            std::exponential_distribution<double> exp(1.0);
            for (size_t i = 0; i < nvalues; ++i)
                w[i] = enabled_values[i] ? (exp(rng) + 1e-12) : 0.0;

            normalizeEnabledInPlace(w, enabled_values);

            const auto [mn, mx] = minmaxEnabled(w, enabled_values);
            const double ratio = (mn > 0.0) ? (mx / mn) : std::numeric_limits<double>::infinity();
            if (ratio <= cfg.balanced_max_ratio)
                return w;
        }

        /// fallback
        std::vector<double> w(nvalues, 0.0);
        std::exponential_distribution<double> exp(1.0);
        for (size_t i = 0; i < nvalues; ++i)
            w[i] = enabled_values[i] ? (exp(rng) + 1e-12) : 0.0;

        normalizeEnabledInPlace(w, enabled_values);
        return w;
    }

    std::vector<double> genBounded()
    {
        std::vector<double> w(nvalues, 0.0);

        for (size_t i = 0; i < nvalues; ++i)
        {
            if (!enabled_values[i])
            {
                w[i] = 0.0;
                continue;
            }
            const auto b = cfg.bounds[i];
            if (b.min < 0.0 || b.max < 0.0 || b.min > b.max)
                throw std::runtime_error("Invalid bounds");
            std::uniform_real_distribution<double> unif(b.min, b.max);
            w[i] = unif(rng);
        }
        normalizeEnabledInPlace(w, enabled_values);
        clampToBoundsAndRenormEnabled(w, cfg.bounds, enabled_values);
        return w;
    }

    void applyDrift()
    {
        /// drift only enabled ops
        std::uniform_real_distribution<double> unif(-cfg.drift_strength, cfg.drift_strength);
        std::vector<double> w = probabilites;

        for (size_t i = 0; i < nvalues; ++i)
        {
            if (!enabled_values[i])
            {
                w[i] = 0.0;
                continue;
            }
            w[i] = w[i] * (1.0 + unif(rng));
            w[i] = std::max(w[i], 0.0);
        }

        normalizeEnabledInPlace(w, enabled_values);
        clampToBoundsAndRenormEnabled(w, cfg.bounds, enabled_values);
        probabilites = w;
    }

    void buildCdf()
    {
        double running = 0.0;

        for (size_t i = 0; i < nvalues; ++i)
        {
            running += probabilites[i];
            cdf[i] = running;
        }
        /// Force last enabled bucket to 1.0; others after it (if disabled) already have same value.
        cdf[nvalues - 1] = 1.0;
    }

    void applyEnabledMaskAndRenorm(std::vector<double> & p) const
    {
        chassert(p.size() == nvalues && enabled_values.size() == nvalues);
        for (size_t i = 0; i < nvalues; ++i)
            if (!enabled_values[i])
                p[i] = 0.0;
        normalizeEnabledInPlace(p, enabled_values);
    }

    void normalizeEnabledInPlace(std::vector<double> & v, const std::vector<bool> & enabled) const
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

    std::pair<double, double> minmaxEnabled(const std::vector<double> & v, const std::vector<bool> & enabled) const
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

    void
    clampToBoundsAndRenormEnabled(std::vector<double> & p, const std::vector<ProbabilityBounds> & bounds, const std::vector<bool> & enabled)
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
        if (sum_min > 1.0 + 1e-12 || sum_max < 1.0 - 1e-12)
            throw std::runtime_error("Inconsistent bounds for enabled subset (cannot sum to 1)");

        std::vector<bool> fixed(nvalues, false);
        for (int iter = 0; iter < 10; ++iter)
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

            if (std::fabs(remaining) < 1e-12)
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
};

}
