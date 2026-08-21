#pragma once

#include <cstdint>
#include <vector>

#include <pcg_random.hpp>

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

class ProbabilityGenerator
{
public:
    const size_t nvalues;
    ProbabilityStrategy strategy = ProbabilityStrategy::BoundedRealism;
    double balanced_max_ratio = 6.0;
    int balanced_resample_attempts = 20;
    /// Drifting controls
    uint64_t drift_every_n_ops = 500;
    double drift_strength = 0.10; /// ±10%
    std::vector<ProbabilityBounds> bounds; /// used by Bounded + Drifting

    explicit ProbabilityGenerator(ProbabilityStrategy ps, uint64_t in_seed, const std::vector<ProbabilityBounds> & b);

    uint64_t getSeed() const;
    ProbabilityStrategy getStrategy() const;
    const std::vector<double> & getProbs() const;
    const std::vector<bool> & getEnabledMask() const;

    /// Change enable mask at runtime. Preserves current distribution shape as much as possible.
    void setEnabled(const std::vector<bool> & mask);
    void setEnabled(size_t i, bool on);

    /// Sample next op (ignores disabled ops automatically)
    size_t nextOp(bool tick = true);
    void tick();

private:
    uint64_t seed;
    pcg64_fast generator;

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

    void normalizeEnabledInPlace(std::vector<double> & v, const std::vector<bool> & enabled_values) const;

    std::pair<double, double> minmaxEnabled(const std::vector<double> & v, const std::vector<bool> & enabled_values) const;

    void clampToBoundsAndRenormEnabled(std::vector<double> & p, const std::vector<bool> & enabled_values);
};

}
