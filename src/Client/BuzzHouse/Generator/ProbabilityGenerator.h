#pragma once

#include <random>
#include <vector>

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

class ProbabilityGenerator
{
public:
    const size_t nvalues;

    explicit ProbabilityGenerator(ProbabilityConfig _cfg)
        : nvalues(_cfg.bounds.size())
        , cfg(std::move(_cfg))
        , seed_used(cfg.seed)
        , cdf(nvalues, 0.0)
        , enabled_values(cfg.enabled)
    {
        rng.seed(seed_used);
        ensureAtLeastOneEnabled(enabled_values);
        probabilities = generateInitial();
        applyEnabledMaskAndRenorm(probabilities);
        buildCdf();
    }

    uint64_t seedUsed() const;
    ProbabilityStrategy strategy() const;
    const std::vector<double> & probs() const;
    const std::vector<bool> & enabled() const;

    /// Change enable mask at runtime. Preserves current distribution shape as much as possible.
    void setEnabled(const std::vector<bool> & mask);
    void setEnabled(size_t i, bool on);

    /// Sample next op (ignores disabled ops automatically)
    size_t nextOp(bool tick = true);
    void tick();

private:
    ProbabilityConfig cfg;
    std::mt19937_64 rng;
    uint64_t seed_used;

    std::vector<double> cdf;
    std::vector<bool> enabled_values;
    std::vector<double> probabilities;
    uint64_t ops_emitted = 0;

    static void ensureAtLeastOneEnabled(const std::vector<bool> & mask);

    size_t lastEnabledEnum() const;

    std::vector<double> generateInitial();

    std::vector<double> genBalanced();

    std::vector<double> genBounded();

    void applyDrift();

    void buildCdf();

    void applyEnabledMaskAndRenorm(std::vector<double> & p) const;

    void normalizeEnabledInPlace(std::vector<double> & v, const std::vector<bool> & enabled) const;

    std::pair<double, double> minmaxEnabled(const std::vector<double> & v, const std::vector<bool> & enabled) const;

    void clampToBoundsAndRenormEnabled(
        std::vector<double> & p, const std::vector<ProbabilityBounds> & bounds, const std::vector<bool> & enabled);
};

}
