#pragma once

#include <cstdint>
#include <random>
#include <vector>

#include <base/types.h>
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

    explicit ProbabilityGenerator(ProbabilityStrategy ps, uint64_t in_seed, const std::vector<ProbabilityBounds> & b, const String & desc);

    uint64_t getSeed() const;
    ProbabilityStrategy getStrategy() const;
    const std::vector<double> & getProbs() const;
    const std::vector<bool> & getEnabledMask() const;

    /// Change enable mask at runtime. Probabilities are re-derived from the unmasked base
    /// distribution, so a re-enabled op gets its share back.
    void setEnabled(const std::vector<bool> & mask);
    void setEnabled(size_t i, bool on);

    /// Sample next op (ignores disabled ops automatically)
    size_t nextOp(bool tick = true);
    void tick();

private:
    /// Numerical tolerance for sum-to-one and feasibility checks. Equality at the level of
    /// double-precision rounding is what we care about; tighter tolerances would falsely
    /// reject legitimate distributions after a normalize-and-renormalize cycle.
    static constexpr double kEpsilon = 1e-12;
    /// Maximum iterations of the water-filling clamp loop. The loop converges in a handful
    /// of passes in practice; the cap is a safety net so a pathological bounds set cannot
    /// spin forever.
    static constexpr int kClampMaxIterations = 10;

    uint64_t seed;
    pcg64_fast generator;
    /// Reused across nextOp() calls so we don't reconstruct the distribution object every sample.
    std::uniform_real_distribution<double> uniform_unit{0.0, 1.0};

    std::vector<double> cdf;
    const std::vector<bool> all_enabled;
    std::vector<bool> enabled_values;
    /// Unmasked distribution over all ops; initialization, drift and bounds act on it.
    std::vector<double> base_probabilities;
    /// Derived from `base_probabilities` and `enabled_values`; this is what `nextOp` samples.
    std::vector<double> probabilities;
    uint64_t ops_emitted = 0;
    const String description;

    static void ensureAtLeastOneEnabled(const std::vector<bool> & mask);

    size_t lastEnabledIndex() const;

    std::vector<double> generateInitial();

    std::vector<double> genBalanced();

    std::vector<double> genBounded();

    void applyDrift();

    void buildCdf();

    std::vector<double> deriveProbabilities(const std::vector<bool> & mask);

    void normalizeEnabledInPlace(std::vector<double> & v, const std::vector<bool> & enabled_values) const;

    std::pair<double, double> minmaxEnabled(const std::vector<double> & v, const std::vector<bool> & enabled_values) const;

    void clampToBoundsAndRenormEnabled(std::vector<double> & p, const std::vector<bool> & enabled_values);
};

}
