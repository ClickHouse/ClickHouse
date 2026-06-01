#pragma once

#include <Compression/Pcodec/Binning.h>
#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>

#include <cmath>
#include <cstdint>
#include <vector>

/** Encoder-side delta encoding (consecutive) + cost-based auto-selection.
  *
  * Ported from tmp/pcodec_ref/pco/src/delta/consecutive.rs. The reference estimates cost on a
  * sample; for simplicity and accuracy we estimate on the full data (encoding is comparatively
  * cheap). We only ever switch away from NoOp when it strictly reduces the estimated size, so the
  * encoder never expands beyond the Classic/NoOp guarantee.
  */
namespace DB::Pcodec
{

/// Delta-selection sampling parameters (chunk_compressor.rs).
inline constexpr size_t DELTA_GROUP_SIZE = 200;
inline constexpr size_t N_PER_EXTRA_DELTA_GROUP = 10000;

/// Consecutive delta-encodes `latents` in place (order applications). Returns the `order` delta
/// moments (raw first values). After this call, `latents[min(order,n)..]` holds the toggled deltas
/// (the "body"); the first `min(order,n)` entries are junk.
template <Latent L>
std::vector<L> encodeConsecutiveInPlace(size_t order, std::vector<L> & latents)
{
    size_t len = latents.size();
    std::vector<L> moments;
    moments.reserve(order);
    size_t start = 0;
    for (size_t o = 0; o < order; ++o)
    {
        moments.push_back(start < len ? latents[start] : L{0});
        for (size_t i = len; i-- > start + 1;)
            latents[i] = static_cast<L>(latents[i] - latents[i - 1]);
        if (start < len)
            ++start;
    }
    for (size_t i = start; i < len; ++i)
        latents[i] = toggleCenter(latents[i]);
    return moments;
}

/// Estimated compressed bit-size of a body given its trained bins (chunk_compressor.rs avg-bits model).
template <Latent L>
double estimateChunkBits(const TrainedBins<L> & trained, size_t body_n, size_t order)
{
    double total_weight = static_cast<double>(uint64_t{1} << trained.ans_size_log);
    double avg_bits = 0;
    for (const auto & info : trained.infos)
    {
        double ans_bits = static_cast<double>(trained.ans_size_log) - std::log2(static_cast<double>(info.weight));
        avg_bits += (ans_bits + static_cast<double>(info.offset_bits)) * static_cast<double>(info.weight) / total_weight;
    }
    double bin_meta_bits = static_cast<double>(trained.infos.size())
        * static_cast<double>(trained.ans_size_log + latentBits<L> + bitsToEncodeOffsetBits<L>());
    double delta_state_bits = static_cast<double>(order) * latentBits<L>;
    return static_cast<double>(body_n) * avg_bits + bin_meta_bits + delta_state_bits;
}

/// The result of choosing a delta encoding: the (possibly delta-encoded) body, its trained bins,
/// the consecutive order (0 == NoOp), and the delta moments.
template <Latent L>
struct ChosenDelta
{
    size_t order = 0;
    std::vector<L> body;
    std::vector<L> moments;
    TrainedBins<L> trained;
};

/// Builds a small, spread-out sample of any value array (chunk_compressor.rs::choose_delta_sample).
/// Used both for delta-order selection (on latents) and mode-cost estimation (on raw values).
template <typename U>
std::vector<U> spreadSample(const std::vector<U> & values)
{
    size_t n = values.size();
    size_t n_extra_groups = 1 + n / N_PER_EXTRA_DELTA_GROUP;
    size_t nominal = (n_extra_groups + 1) * DELTA_GROUP_SIZE;
    if (n <= nominal)
        return values;

    size_t group_padding = (n - nominal) / n_extra_groups;
    std::vector<U> sample;
    sample.reserve(nominal);
    for (size_t k = 0; k < DELTA_GROUP_SIZE; ++k)
        sample.push_back(values[k]);
    size_t i = DELTA_GROUP_SIZE;
    for (size_t g = 0; g < n_extra_groups; ++g)
    {
        i += group_padding;
        for (size_t k = 0; k < DELTA_GROUP_SIZE && i + k < n; ++k)
            sample.push_back(values[i + k]);
        i += DELTA_GROUP_SIZE;
    }
    return sample;
}

/// Builds a small, spread-out sample of the latents for cheap delta-order selection
/// (chunk_compressor.rs::choose_delta_sample).
template <Latent L>
std::vector<L> chooseDeltaSample(const std::vector<L> & latents)
{
    size_t n = latents.size();
    size_t n_extra_groups = 1 + n / N_PER_EXTRA_DELTA_GROUP;
    size_t nominal = (n_extra_groups + 1) * DELTA_GROUP_SIZE;
    if (n <= nominal)
        return latents;

    size_t group_padding = (n - nominal) / n_extra_groups;
    std::vector<L> sample;
    sample.reserve(nominal);
    for (size_t k = 0; k < DELTA_GROUP_SIZE; ++k)
        sample.push_back(latents[k]);
    size_t i = DELTA_GROUP_SIZE;
    for (size_t g = 0; g < n_extra_groups; ++g)
    {
        i += group_padding;
        for (size_t k = 0; k < DELTA_GROUP_SIZE && i + k < n; ++k)
            sample.push_back(latents[i + k]);
        i += DELTA_GROUP_SIZE;
    }
    return sample;
}

/// Estimated cost (bits) of compressing `sample` under the given consecutive delta order.
template <Latent L>
double sampleDeltaCost(const std::vector<L> & sample, size_t order, Bitlen unoptimized_bins_log)
{
    if (order == 0)
    {
        TrainedBins<L> trained = trainInfos(sample, unoptimized_bins_log);
        return estimateChunkBits(trained, sample.size(), 0);
    }
    std::vector<L> work = sample;
    encodeConsecutiveInPlace<L>(order, work);
    std::vector<L> body(work.begin() + std::min(order, work.size()), work.end());
    TrainedBins<L> trained = trainInfos(body, unoptimized_bins_log);
    return estimateChunkBits(trained, body.size(), order);
}

/// Cheap, sample-based estimate of bits-per-number for a latent variable (used to compare a
/// candidate mode against Classic without doing a full encode of each). `allow_delta` enables
/// trying consecutive delta orders (used for the primary; the secondary is always NoOp).
template <Latent L>
double estimateCostPerNum(const std::vector<L> & latents, Bitlen unoptimized_bins_log, bool allow_delta)
{
    if (latents.empty())
        return 0.0;
    std::vector<L> sample = chooseDeltaSample(latents);
    double best = sampleDeltaCost(sample, 0, unoptimized_bins_log);
    if (allow_delta)
        for (size_t order = 1; order <= MAX_CONSECUTIVE_DELTA_ORDER && order < sample.size(); ++order)
        {
            double cost = sampleDeltaCost(sample, order, unoptimized_bins_log);
            if (cost < best)
                best = cost;
            else
                break;
        }
    return best / static_cast<double>(sample.size());
}

/// Chooses between NoOp and consecutive delta orders 1..7 (chunk_compressor.rs). The order is
/// selected cheaply on a small sample; the full array is delta-encoded only once for the winner.
/// Takes `latents` by value: for the common NoOp (order 0) path the input is moved straight into
/// the body (no copy); for delta orders it is consumed in place. `trainInfos` makes its own sorted
/// copy, so `best.body` stays in original order for the later dissect pass.
template <Latent L>
ChosenDelta<L> chooseDelta(std::vector<L> latents, Bitlen unoptimized_bins_log)
{
    size_t best_order = 0;
    if (latents.size() > 1)
    {
        std::vector<L> sample = chooseDeltaSample(latents);
        double best_cost = sampleDeltaCost(sample, 0, unoptimized_bins_log);
        for (size_t order = 1; order <= MAX_CONSECUTIVE_DELTA_ORDER && order < sample.size(); ++order)
        {
            double cost = sampleDeltaCost(sample, order, unoptimized_bins_log);
            if (cost < best_cost)
            {
                best_order = order;
                best_cost = cost;
            }
            else
            {
                // The cost is almost always convex in the order, so stop at the first increase.
                break;
            }
        }
    }

    ChosenDelta<L> best;
    best.order = best_order;
    if (best_order == 0)
    {
        best.body = std::move(latents);
    }
    else
    {
        best.moments = encodeConsecutiveInPlace<L>(best_order, latents);
        best.body.assign(latents.begin() + std::min(best_order, latents.size()), latents.end());
    }
    best.trained = trainInfos(best.body, unoptimized_bins_log);
    return best;
}

}
