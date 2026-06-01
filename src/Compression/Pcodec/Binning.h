#pragma once

#include <Compression/Pcodec/Ans.h>
#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

/** Binning for the encoder: build a histogram of the latents, then merge bins optimally.
  *
  * Ported from tmp/pcodec_ref/pco/src/{histograms.rs,bin_optimization.rs}. The reference uses a
  * partial quicksort for speed; we use a full std::sort followed by the same `apply_sorted` logic,
  * which produces equivalent bins (the encoder need not match the reference byte-for-byte, only
  * produce a spec-valid stream). The bin-optimization DP uses std::log2 instead of the reference's
  * fast approximation for the same reason.
  */
namespace DB::Pcodec
{

/// Bin-merging shortcut thresholds (bin_optimization.rs).
inline constexpr float SINGLE_BIN_SPEEDUP = 0.1f;
inline constexpr float TRIVIAL_OFFSET_SPEEDUP = 0.1f;

template <Latent L>
struct HistogramBin
{
    size_t count;
    L lower;
    L upper;
};

template <Latent L>
struct BinCompressionInfo
{
    Weight weight; // raw count until quantization, then the ANS weight
    L lower;
    L upper;
    Bitlen offset_bits;
    Symbol symbol;
};

// ---- Partial-sort utilities (ported from sort_utils.rs) for the histogram quicksort ----

/// Median-of-medians / median-of-three pivot selection (read-only; returns a pivot value).
template <Latent L>
L histChoosePivot(const L * v, size_t len)
{
    size_t a = len / 4;
    size_t b = len / 2;
    size_t c = (len * 3) / 4;
    if (len >= 8)
    {
        auto sort2 = [&](size_t & x, size_t & y) { if (v[y] < v[x]) std::swap(x, y); };
        auto sort3 = [&](size_t & x, size_t & y, size_t & z) { sort2(x, y); sort2(y, z); sort2(x, y); };
        if (len >= 50)
        {
            auto sort_adjacent = [&](size_t & m) { size_t lo = m - 1; size_t hi = m + 1; sort3(lo, m, hi); };
            sort_adjacent(a);
            sort_adjacent(b);
            sort_adjacent(c);
        }
        sort3(a, b, c);
    }
    return v[b];
}

/// Branchless Lomuto partition around `pivot`. Returns (count of elements < pivot, was-the-pivot-bad).
/// The swap is unconditional (no data-dependent branch); a BlockQuicksort-style block partition was
/// tried and measured no faster here (the branchless Lomuto already avoids mispredictions).
template <Latent L>
std::pair<size_t, bool> histPartition(L * v, size_t len, L pivot)
{
    size_t left = 0;
    for (size_t pos = 0; pos < len; ++pos)
    {
        L value = v[pos];
        bool is_lt = value < pivot;
        v[pos] = v[left];
        v[left] = value;
        left += is_lt ? 1 : 0;
    }
    bool was_bad = 1 + std::min(left, len - left) < len / 8;
    return {left, was_bad};
}

/// Scatter a few elements to break adversarial patterns that cause imbalanced partitions.
template <Latent L>
void histBreakPatterns(L * v, size_t len)
{
    if (len < 8)
        return;
    size_t seed = len;
    auto gen = [&]() { size_t r = seed; r ^= r << 13; r ^= r >> 7; r ^= r << 17; seed = r; return r; };
    size_t modulus = std::bit_ceil(len);
    size_t pos = len / 4 * 2;
    for (size_t i = 0; i < 3; ++i)
    {
        size_t other = gen() & (modulus - 1);
        if (other >= len)
            other -= len;
        std::swap(v[pos - 1 + i], v[other]);
    }
}

/// Worst-case O(n log n) heapsort fallback.
template <Latent L>
void histHeapsort(L * v, size_t len)
{
    auto sift_down = [&](size_t cnt, size_t node)
    {
        while (true)
        {
            size_t child = 2 * node + 1;
            if (child >= cnt)
                break;
            if (child + 1 < cnt)
                child += (v[child] < v[child + 1]) ? 1 : 0;
            if (!(v[node] < v[child]))
                break;
            std::swap(v[node], v[child]);
            node = child;
        }
    };
    for (size_t i = len / 2; i-- > 0;)
        sift_down(len, i);
    for (size_t i = len; i-- > 1;)
    {
        std::swap(v[0], v[i]);
        sift_down(i, 0);
    }
}

template <Latent L>
class HistogramBuilder
{
public:
    HistogramBuilder(size_t n_, Bitlen n_bins_log_) : n(n_), n_bins(uint64_t{1} << n_bins_log_), n_bins_log(n_bins_log_) { }

    std::vector<HistogramBin<L>> dst;

    /// Partial quicksort that only sorts down to bin boundaries (histograms.rs::apply_quicksort_recurse).
    /// `lower_loose`/`upper_loose` mark whether the current range's bounds are loose (need a scan) or
    /// tight (a known value). Far cheaper than a full sort for a small number of bins.
    void applyQuicksort(L * data, size_t len, L lb, bool lb_tight, L ub, bool ub_tight, uint32_t bad_pivot_limit)
    {
        if (len == 0)
            return;

        size_t target_bin_idx = binIdx(n_applied);
        size_t target_c_count = cCount(target_bin_idx);
        size_t end = n_applied + len;
        if (end <= target_c_count)
        {
            applyIncomplete(data, len, lb, lb_tight, ub, ub_tight);
            if (end == target_c_count)
                completeBin(target_bin_idx);
            return;
        }

        if (lb == ub || len == 1)
        {
            applyConstantRun(data, len);
            return;
        }

        L tentative = histChoosePivot(data, len);
        L pivot;
        L lhs_ub;
        bool lhs_ub_tight;
        L rhs_lb;
        bool rhs_lb_tight;
        if (tentative > lb)
        {
            pivot = tentative;
            lhs_ub = static_cast<L>(tentative - 1);
            lhs_ub_tight = false;
            rhs_lb = tentative;
            rhs_lb_tight = true;
        }
        else
        {
            pivot = static_cast<L>(tentative + 1);
            lhs_ub = tentative;
            lhs_ub_tight = true;
            rhs_lb = static_cast<L>(tentative + 1);
            rhs_lb_tight = false;
        }

        auto [lhs_count, was_bad] = histPartition(data, len, pivot);
        if (was_bad)
        {
            --bad_pivot_limit;
            if (bad_pivot_limit == 0)
            {
                histHeapsort(data, lhs_count);
                histHeapsort(data + lhs_count, len - lhs_count);
                applySorted(data, len);
                return;
            }
            histBreakPatterns(data, lhs_count);
            histBreakPatterns(data + lhs_count, len - lhs_count);
        }
        applyQuicksort(data, lhs_count, lb, lb_tight, lhs_ub, lhs_ub_tight, bad_pivot_limit);
        applyQuicksort(data + lhs_count, len - lhs_count, rhs_lb, rhs_lb_tight, ub, ub_tight, bad_pivot_limit);
    }

private:
    uint64_t n;
    uint64_t n_bins;
    Bitlen n_bins_log;
    size_t n_applied = 0;
    size_t next_avail_bin_idx = 0;
    bool incomplete_present = false;
    HistogramBin<L> incomplete{};

    size_t binIdx(size_t c_count) const { return static_cast<size_t>((static_cast<uint64_t>(c_count) << n_bins_log) / n); }
    size_t cCount(size_t bin_idx) const { return static_cast<size_t>(((static_cast<uint64_t>(bin_idx) + 1) * n + n_bins - 1) >> n_bins_log); }

    static L sliceMin(const L * data, size_t len)
    {
        L m = data[0];
        for (size_t i = 1; i < len; ++i)
            m = std::min(m, data[i]);
        return m;
    }
    static L sliceMax(const L * data, size_t len)
    {
        L m = data[0];
        for (size_t i = 1; i < len; ++i)
            m = std::max(m, data[i]);
        return m;
    }

    void applyIncomplete(const L * data, size_t len, L lb, bool lb_tight, L ub, bool ub_tight)
    {
        if (len == 0)
            return;
        if (incomplete_present)
        {
            incomplete.upper = ub_tight ? ub : sliceMax(data, len);
            incomplete.count += len;
        }
        else
        {
            L lower = lb_tight ? lb : sliceMin(data, len);
            L upper = ub_tight ? ub : sliceMax(data, len);
            incomplete = HistogramBin<L>{len, lower, upper};
            incomplete_present = true;
        }
        n_applied += len;
    }

    bool completeBin(size_t bin_idx)
    {
        if (incomplete_present)
        {
            next_avail_bin_idx = bin_idx + 1;
            dst.push_back(incomplete);
            incomplete_present = false;
            return true;
        }
        return false;
    }

    void applyConstantRun(const L * data, size_t len)
    {
        L value = data[0];
        size_t start = n_applied;
        size_t mid = start + len / 2;
        size_t end = start + len;
        size_t bin_idx = binIdx(mid);
        if (bin_idx > next_avail_bin_idx)
        {
            size_t spare_bin_idx = bin_idx - 1;
            if (!completeBin(spare_bin_idx))
                bin_idx = spare_bin_idx;
        }
        applyIncomplete(data, len, value, true, value, true);
        if (end >= cCount(bin_idx))
            completeBin(bin_idx);
    }

    /// Fallback used after a heapsort on a fully-sorted range.
    void applySorted(const L * data, size_t len)
    {
        size_t pos = 0;
        while (pos < len)
        {
            size_t target_bin_idx = binIdx(n_applied);
            size_t target_c_count = cCount(target_bin_idx);
            size_t target_i = target_c_count - n_applied;
            size_t remaining = len - pos;
            if (target_i >= remaining)
            {
                applyIncomplete(data + pos, remaining, data[pos], true, data[pos + remaining - 1], true);
                if (target_i == remaining)
                    completeBin(target_bin_idx);
                break;
            }
            size_t l = target_i - 1;
            size_t r = target_i;
            L target_x = data[pos + l];
            while (l > 0 && data[pos + l - 1] == target_x)
                --l;
            while (r < remaining && data[pos + r] == target_x)
                ++r;
            if (l > 0)
                applyIncomplete(data + pos, l, data[pos], true, data[pos + l - 1], true);
            applyConstantRun(data + pos + l, r - l);
            pos += r;
        }
        if (incomplete_present)
        {
            dst.push_back(incomplete);
            incomplete_present = false;
        }
    }
};

template <Latent L>
std::vector<HistogramBin<L>> histogram(std::vector<L> & latents, Bitlen n_bins_log)
{
    size_t len = latents.size();
    if (len == 0)
        return {};
    HistogramBuilder<L> builder(len, n_bins_log);
    uint32_t bad_pivot_limit = 1 + static_cast<uint32_t>(std::bit_width(len + 1) - 1);
    builder.applyQuicksort(latents.data(), len, L{0}, false, std::numeric_limits<L>::max(), false, bad_pivot_limit);
    return std::move(builder.dst);
}

template <Latent L>
inline Bitlen binExactBitSize(Bitlen ans_size_log)
{
    return ans_size_log + latentBits<L> + bitsToEncodeOffsetBits<L>();
}

template <Latent L>
inline float binCost(float bin_meta_cost, L lower, L upper, Weight count, float total_count_log2)
{
    float ans_cost = total_count_log2 - std::log2(static_cast<float>(count));
    float offset_cost = static_cast<float>(bitsToEncodeOffset(static_cast<L>(upper - lower)));
    return bin_meta_cost + (ans_cost + offset_cost) * static_cast<float>(count);
}

/// Exactly-optimal merging of consecutive histogram bins (bin_optimization.rs).
template <Latent L>
std::vector<BinCompressionInfo<L>> optimizeBins(const std::vector<HistogramBin<L>> & bins, Bitlen ans_size_log)
{
    size_t n = bins.size();
    float bin_meta_cost = static_cast<float>(binExactBitSize<L>(ans_size_log));

    std::vector<uint64_t> c_counts(n + 1, 0);
    std::vector<float> best_costs(n + 1, 0.0f);
    for (size_t i = 0; i < n; ++i)
        c_counts[i + 1] = c_counts[i] + bins[i].count;
    uint64_t total_count = c_counts[n];
    float total_count_log2 = std::log2(static_cast<float>(total_count));

    std::vector<size_t> best_js(n);
    for (size_t i = 0; i < n; ++i)
    {
        float best_cost = std::numeric_limits<float>::max();
        size_t best_j = 0;
        L upper = bins[i].upper;
        uint64_t c_count_i = c_counts[i + 1];
        for (size_t j = i + 1; j-- > 0;)
        {
            L lower = bins[j].lower;
            float cost = best_costs[j]
                + binCost<L>(bin_meta_cost, lower, upper, static_cast<Weight>(c_count_i - c_counts[j]), total_count_log2);
            if (cost < best_cost)
            {
                best_cost = cost;
                best_j = j;
            }
        }
        best_costs[i + 1] = best_cost;
        best_js[i] = best_j;
    }
    float best_cost = best_costs[n];

    // Shortcut 1: a single bin covering everything.
    float single_bin_cost = binCost<L>(bin_meta_cost, bins[0].lower, bins[n - 1].upper, static_cast<Weight>(total_count), total_count_log2);
    std::vector<std::pair<size_t, size_t>> partitioning;
    if (single_bin_cost < best_cost + SINGLE_BIN_SPEEDUP * static_cast<float>(total_count))
    {
        partitioning.emplace_back(0, n - 1);
    }
    else
    {
        // Shortcut 2: one bin per distinct value when all bins are trivial (lower == upper).
        bool all_trivial = true;
        for (const auto & b : bins)
            if (b.lower != b.upper)
            {
                all_trivial = false;
                break;
            }
        bool used_trivial = false;
        if (all_trivial)
        {
            float trivial_cost = 0;
            for (const auto & b : bins)
                trivial_cost += binCost<L>(bin_meta_cost, b.lower, b.upper, static_cast<Weight>(b.count), total_count_log2);
            if (trivial_cost < best_cost + TRIVIAL_OFFSET_SPEEDUP * static_cast<float>(total_count))
            {
                for (size_t i = 0; i < n; ++i)
                    partitioning.emplace_back(i, i);
                used_trivial = true;
            }
        }
        if (!used_trivial)
        {
            // Rewind the DP.
            size_t i = n - 1;
            while (true)
            {
                size_t j = best_js[i];
                partitioning.emplace_back(j, i);
                if (j > 0)
                    i = j - 1;
                else
                    break;
            }
            std::reverse(partitioning.begin(), partitioning.end());
        }
    }

    std::vector<BinCompressionInfo<L>> res;
    res.reserve(partitioning.size());
    for (size_t symbol = 0; symbol < partitioning.size(); ++symbol)
    {
        auto [j, i] = partitioning[symbol];
        size_t count = 0;
        for (size_t b = j; b <= i; ++b)
            count += bins[b].count;
        res.push_back(BinCompressionInfo<L>{
            static_cast<Weight>(count), bins[j].lower, bins[i].upper,
            bitsToEncodeOffset(static_cast<L>(bins[i].upper - bins[j].lower)), static_cast<Symbol>(symbol)});
    }
    return res;
}

template <Latent L>
struct TrainedBins
{
    std::vector<BinCompressionInfo<L>> infos;
    Bitlen ans_size_log = 0;
    std::vector<Weight> counts;
};

/// Trains bins on a (copied, will be sorted) latent array (chunk_compressor.rs::train_infos).
template <Latent L>
TrainedBins<L> trainInfos(std::vector<L> latents, Bitlen unoptimized_bins_log)
{
    TrainedBins<L> result;
    if (latents.empty())
        return result;

    size_t n_latents = latents.size();
    std::vector<HistogramBin<L>> unoptimized = histogram(latents, unoptimized_bins_log);

    Bitlen n_log_ceil = n_latents <= 1 ? 0 : static_cast<Bitlen>(32 - std::countl_zero(static_cast<uint32_t>(n_latents - 1)));
    Bitlen estimated_ans_size_log = std::min({unoptimized_bins_log + 2, static_cast<Bitlen>(MAX_COMPRESSION_LEVEL), n_log_ceil});

    result.infos = optimizeBins(unoptimized, estimated_ans_size_log);
    result.counts.reserve(result.infos.size());
    for (const auto & info : result.infos)
        result.counts.push_back(info.weight);

    auto [ans_size_log, weights] = quantizeWeights(result.counts, n_latents, estimated_ans_size_log);
    result.ans_size_log = ans_size_log;
    for (size_t i = 0; i < weights.size(); ++i)
        result.infos[i].weight = weights[i];
    return result;
}

}
