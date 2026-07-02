#pragma once

#include <Compression/Pcodec/PcoArray.h>

#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/NumberTraits.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <optional>
#include <unordered_map>
#include <vector>

/** Encoder-side mode detection and latent splitting (ports of mode/{classic,int_mult,float_quant}.rs
  * and sampling.rs). Currently supports Classic, IntMult and FloatQuant on the encode side;
  * FloatMult auto-detection (the approximate-Euclidean base search) is not yet ported, but the
  * decoder supports all modes. Mode choice only affects ratio, never correctness — the wire format
  * is validated against the reference regardless of which mode is picked.
  */
namespace DB::Pcodec
{

inline constexpr size_t MIN_SAMPLE = 10;
inline constexpr size_t SAMPLE_RATIO = 40;
inline constexpr double ZETA_OF_2 = 1.6449340668482264; // pi^2 / 6

inline double singleCategoryEntropy(double p)
{
    return (p <= 0.0 || p >= 1.0) ? 0.0 : -p * std::log2(p);
}

/// Worst-case entropy of a categorical distribution with one value at probability `cp` and the
/// remaining `m1` values sharing the rest uniformly (mode/mod.rs).
inline double worstCaseCategoricalEntropy(double cp, double m1)
{
    if (m1 <= 0.0)
        return singleCategoryEntropy(cp);
    return singleCategoryEntropy(cp) + m1 * singleCategoryEntropy((1.0 - cp) / m1);
}

inline size_t calcSampleN(size_t n)
{
    return n >= MIN_SAMPLE ? MIN_SAMPLE + (n - MIN_SAMPLE) / SAMPLE_RATIO : 0;
}

/// Deterministic spread sample (we don't need the reference's RNG — sample quality only affects
/// ratio). `f` maps a value to an optional sample element (used to filter, e.g., non-normal floats).
template <typename T, typename S, typename F>
PcoArray<S> chooseSample(const PcoArray<T> & vals, F && f)
{
    size_t target = calcSampleN(vals.size());
    if (target == 0)
        return {};
    PcoArray<S> res;
    res.reserve(target);
    size_t stride = std::max<size_t>(1, vals.size() / (target * 3 + 1));
    for (size_t i = 0; i < vals.size() && res.size() < target; i += stride)
        if (auto s = f(vals[i]))
            res.push_back(*s);
    if (res.size() < MIN_SAMPLE) // fall back to a full scan if the stride filtered too much
    {
        res.clear();
        for (size_t i = 0; i < vals.size() && res.size() < target; ++i)
            if (auto s = f(vals[i]))
                res.push_back(*s);
    }
    return res.size() >= MIN_SAMPLE ? res : PcoArray<S>{};
}

/// Average bits saved per number, counting only "infrequent" primary latents (sampling.rs).
template <Latent L>
double estBitsSavedPerNum(const PcoArray<L> & primaries, double bits_saved_per_infrequent)
{
    std::unordered_map<L, size_t> counts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (L p : primaries)
        ++counts[p];
    size_t cutoff = std::max<size_t>(1, primaries.size() >> CLASSIC_MEMORIZABLE_BINS_LOG);
    size_t infrequent = 0;
    for (const auto & [p, c] : counts)
        if (c <= cutoff)
            infrequent += c;
    return static_cast<double>(infrequent) * bits_saved_per_infrequent / static_cast<double>(primaries.size());
}

// ---- FloatQuant detection (mode/float_quant.rs) ----

template <typename T>
std::pair<Bitlen, double> estimateBestK(const PcoArray<T> & sample)
{
    using TR = NumberTraits<T>;
    using U = typename TR::Latent;
    Bitlen precision = TR::PRECISION_BITS;
    PcoArray<uint32_t> hist(precision + 1, 0);
    for (T x : sample)
    {
        Bitlen tz = std::min<Bitlen>(precision, static_cast<Bitlen>(std::countr_zero(std::bit_cast<U>(x))));
        ++hist[tz];
    }
    // Cumulative from the top: hist[k] = number of samples with at least k trailing mantissa zeros.
    uint32_t rev = 0;
    for (size_t k = hist.size(); k-- > 0;)
    {
        rev += hist[k];
        hist[k] = rev;
    }

    double len = static_cast<double>(sample.size());
    Bitlen best_k = 0;
    double best_saved = 0.0;
    for (size_t k = 1; k < hist.size(); ++k)
    {
        if (hist[k] == 0)
            continue;
        double freq = static_cast<double>(hist[k]) / len;
        double n_categories = static_cast<double>((uint64_t{1} << k) - 1);
        double saved = static_cast<double>(k) - worstCaseCategoricalEntropy(freq, n_categories);
        if (saved > best_saved)
        {
            best_k = static_cast<Bitlen>(k);
            best_saved = saved;
        }
        else
            break;
    }
    return {best_k, best_saved};
}

// ---- IntMult detection (mode/int_mult.rs) ----

template <Latent L>
L calcGcd(L a, L b)
{
    while (true)
    {
        if (a == 0)
            return b;
        b %= a;
        std::swap(a, b);
    }
}

template <Latent L>
L calcTripleGcd(L a, L b, L c)
{
    if (a > b)
        std::swap(a, b);
    if (b > c)
        std::swap(b, c);
    if (a > b)
        std::swap(a, b);
    return calcGcd<L>(static_cast<L>(b - a), static_cast<L>(c - a));
}

template <typename F>
std::optional<double> solveRootByFalsePosition(F && f, double lb, double ub)
{
    constexpr double x_tolerance = 1e-4;
    double flb = f(lb);
    double fub = f(ub);
    if (flb > 0.0 || fub < 0.0)
        return std::nullopt;
    while (ub - lb > x_tolerance && fub - flb > 0.0)
    {
        double lb_prop = 0.001 + 0.998 * fub / (fub - flb);
        double mid = lb_prop * lb + (1.0 - lb_prop) * ub;
        double fmid = f(mid);
        if (fmid < 0.0)
        {
            lb = mid;
            flb = fmid;
        }
        else
        {
            ub = mid;
            fub = fmid;
        }
    }
    return (lb + ub) / 2.0;
}

/// Statistical score (worst-case bits saved) for a candidate GCD, or nullopt if not significant.
inline std::optional<double> filterScoreTripleGcd(double gcd, size_t triples_w_gcd, size_t total_triples)
{
    double tw = static_cast<double>(triples_w_gcd);
    double total = static_cast<double>(total_triples);
    double prob = tw / total;
    double natural = 1.0 / (ZETA_OF_2 * gcd * gcd);
    double stdev = std::sqrt(natural * (1.0 - natural) / total);
    double z = (prob - natural) / stdev;
    if (z < 3.0)
        return std::nullopt;

    double tw_lcb = tw - std::sqrt(tw); // LCB_RATIO = 1.0
    if (tw_lcb <= 0.0)
        return std::nullopt;
    double congruence_lcb = std::min(1.0, ZETA_OF_2 * tw_lcb / total);

    double gcd_m1 = gcd - 1.0;
    double gcd_m1_inv_sq = 1.0 / (gcd_m1 * gcd_m1);
    auto poly = [&](double p) { return p * p * p + (1.0 - p) * (1.0 - p) * (1.0 - p) * gcd_m1_inv_sq - congruence_lcb; };
    double lb = 1.0 / gcd;
    double ub = std::cbrt(congruence_lcb) + std::numeric_limits<double>::epsilon();
    auto cp = solveRootByFalsePosition(poly, lb, ub);
    if (!cp)
        return std::nullopt;
    double entropy = worstCaseCategoricalEntropy(*cp, gcd_m1);
    double saved = std::log2(gcd) - entropy;
    return saved < MULT_REQUIRED_BITS_SAVED_PER_NUM ? std::nullopt : std::optional<double>(saved);
}

template <Latent L>
std::optional<std::pair<L, double>> chooseIntMultBase(const PcoArray<L> & sample)
{
    std::unordered_map<L, size_t> counts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t total_triples = sample.size() / 3;
    for (size_t i = 0; i + 3 <= sample.size(); i += 3)
    {
        L g = calcTripleGcd<L>(sample[i], sample[i + 1], sample[i + 2]);
        if (g > 1)
            ++counts[g];
    }
    std::optional<std::pair<L, double>> best;
    for (const auto & [gcd, count] : counts)
    {
        double gcd_f = static_cast<double>(gcd);
        auto score = filterScoreTripleGcd(gcd_f, count, total_triples);
        if (score && (!best || *score > best->second))
            best = std::make_pair(gcd, *score);
    }
    return best;
}

// ---- Mode detection (cheap, sample-based) and latent splitting (expensive, winner only) ----

struct ModeInfo
{
    ModeVariant variant = ModeVariant::Classic;
    uint64_t base_latent = 0;
    Bitlen k = 0;
    bool has_secondary = false;
};

/// Detects a candidate mode on a small sample. Does NOT split the (full) latents — that is done
/// only for the chosen mode by `splitForMode`, so a rejected mode costs no full-array work (in
/// particular, no 2M-element hardware divisions for IntMult).
template <typename T>
ModeInfo detectMode(const PcoArray<T> & vals)
{
    using TR = NumberTraits<T>;
    using L = typename TR::Latent;
    ModeInfo info;

    if constexpr (std::is_floating_point_v<T>)
    {
        auto sample = chooseSample<T, T>(vals, [](T x) -> std::optional<T>
        {
            if (std::isnormal(x) && std::abs(x) <= std::numeric_limits<T>::max() * T(0.5))
                return std::abs(x);
            return std::nullopt;
        });
        if (!sample.empty())
        {
            auto [k, bsi] = estimateBestK<T>(sample);
            if (k > 0 && k <= TR::PRECISION_BITS)
            {
                PcoArray<L> primaries(sample.size());
                for (size_t i = 0; i < sample.size(); ++i)
                    primaries[i] = static_cast<L>(std::bit_cast<L>(sample[i]) >> k);
                if (estBitsSavedPerNum<L>(primaries, bsi) > QUANT_REQUIRED_BITS_SAVED_PER_NUM)
                {
                    info.variant = ModeVariant::FloatQuant;
                    info.k = k;
                    info.has_secondary = true;
                }
            }
        }
    }
    else if constexpr (std::is_integral_v<T>)
    {
        auto sample = chooseSample<T, L>(vals, [](T x) -> std::optional<L> { return TR::toLatentOrdered(x); });
        if (!sample.empty())
        {
            if (auto base_and_saved = chooseIntMultBase<L>(sample))
            {
                L base = base_and_saved->first;
                PcoArray<L> primaries(sample.size());
                for (size_t i = 0; i < sample.size(); ++i)
                    primaries[i] = static_cast<L>(sample[i] / base);
                if (estBitsSavedPerNum<L>(primaries, base_and_saved->second) > MULT_REQUIRED_BITS_SAVED_PER_NUM)
                {
                    info.variant = ModeVariant::IntMult;
                    info.base_latent = base;
                    info.has_secondary = true;
                }
            }
        }
    }
    return info;
}

/// Splits `n` values (read from `vals`) into primary (+ secondary) latents for the given mode.
/// Works on either the full array (the winner) or a sample (for cost estimation).
template <typename T, typename L = typename NumberTraits<T>::Latent>
void splitForMode(const T * vals, size_t n, const ModeInfo & info, PcoArray<L> & primary, PcoArray<L> & secondary)
{
    using TR = NumberTraits<T>;
    primary.resize(n);
    if (info.variant == ModeVariant::IntMult)
    {
        L base = static_cast<L>(info.base_latent);
        secondary.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            L u = TR::toLatentOrdered(vals[i]);
            L mult = static_cast<L>(u / base);
            primary[i] = mult;
            secondary[i] = static_cast<L>(u - mult * base);
        }
    }
    else if (info.variant == ModeVariant::FloatQuant)
    {
        Bitlen k = info.k;
        L lowest_k_bits_max = static_cast<L>((L{1} << k) - L{1});
        secondary.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            L u = TR::toLatentOrdered(vals[i]);
            primary[i] = static_cast<L>(u >> k);
            L low = static_cast<L>(u & lowest_k_bits_max);
            bool is_pos = (std::bit_cast<L>(vals[i]) & latentMid<L>) == 0;
            secondary[i] = is_pos ? low : static_cast<L>(lowest_k_bits_max - low);
        }
    }
    else // Classic
    {
        for (size_t i = 0; i < n; ++i)
            primary[i] = TR::toLatentOrdered(vals[i]);
    }
}

}
