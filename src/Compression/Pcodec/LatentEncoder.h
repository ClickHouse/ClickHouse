#pragma once

#include <Compression/Pcodec/PcoArray.h>

#include <Compression/Pcodec/Ans.h>
#include <Compression/Pcodec/Binning.h>
#include <Compression/Pcodec/BitWriter.h>
#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/Metadata.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <vector>

/** Encoder side of a single latent variable: dissect (bin search -> offsets -> reverse ANS) and
  * write the page body. Ported from tmp/pcodec_ref/pco/src/{compression_table,chunk_latent_compressor}.rs.
  *
  * The reference's branchless batch search is replaced with a straightforward binary search over
  * the sorted bin lowers (same result; the encoder need not match the reference byte-for-byte).
  */
namespace DB::Pcodec
{

template <Latent L>
struct DissectedVar
{
    PcoArray<AnsState> ans_vals;
    PcoArray<Bitlen> ans_bits;
    PcoArray<L> offsets;
    PcoArray<Bitlen> offset_bits;
    std::array<AnsState, ANS_INTERLEAVING> ans_final_states{};
};

template <Latent L>
class LatentEncoder
{
public:
    PcoArray<BinCompressionInfo<L>> infos_by_lower; // sorted by lower
    PcoArray<L> sorted_lowers;
    PcoArray<L> search_lowers; // padded to 2^search_size_log with L::MAX, for branchless search
    size_t search_size_log = 0;
    AnsEncoder encoder;
    Bitlen ans_size_log = 0;
    bool needs_ans = false;
    bool is_trivial = false; // page body always empty (all latents in one zero-width bin)
    size_t max_u64s_per_offset = 0;

    /// `bins` are in symbol order (used for the ANS spec + bin metadata). `infos` are the same
    /// bins with raw->quantized weights, used for the search table.
    static LatentEncoder build(const TrainedBins<L> & trained, const PcoArray<Bin> & bins)
    {
        LatentEncoder enc;
        enc.ans_size_log = trained.ans_size_log;
        enc.needs_ans = bins.size() != 1;
        enc.is_trivial = bins.empty() || (bins.size() == 1 && bins[0].offset_bits == 0);

        Bitlen max_offset_bits = 0;
        PcoArray<Weight> weights(bins.size());
        for (size_t i = 0; i < bins.size(); ++i)
        {
            weights[i] = bins[i].weight;
            max_offset_bits = std::max(max_offset_bits, bins[i].offset_bits);
        }
        enc.max_u64s_per_offset = calcMaxU64s(max_offset_bits);

        AnsSpec spec = AnsSpec::fromWeights(trained.ans_size_log, weights);
        enc.encoder = AnsEncoder::make(spec);

        enc.infos_by_lower = trained.infos;
        std::sort(enc.infos_by_lower.begin(), enc.infos_by_lower.end(),
                  [](const BinCompressionInfo<L> & a, const BinCompressionInfo<L> & b) { return a.lower < b.lower; });
        size_t nb = enc.infos_by_lower.size();
        enc.sorted_lowers.resize(nb);
        for (size_t i = 0; i < nb; ++i)
            enc.sorted_lowers[i] = enc.infos_by_lower[i].lower;
        // Balanced-binary-tree search table, padded with MAX (compression_table.rs).
        enc.search_size_log = nb <= 1 ? 0 : (1 + static_cast<size_t>(std::bit_width(nb - 1) - 1));
        enc.search_lowers = enc.sorted_lowers;
        enc.search_lowers.resize(size_t{1} << enc.search_size_log, std::numeric_limits<L>::max());
        return enc;
    }

    /// Branchless balanced-tree search over a batch: writes the bin index for each latent into
    /// `search_idx` (compression_table.rs::binary_search). Vectorizes well.
    void searchBatch(const L * latents, size_t batch_n, size_t * search_idx) const
    {
        for (size_t k = 0; k < batch_n; ++k)
            search_idx[k] = 0;
        for (size_t depth = 0; depth < search_size_log; ++depth)
        {
            size_t bisect = size_t{1} << (search_size_log - 1 - depth);
            const L * lowers = search_lowers.data();
            for (size_t k = 0; k < batch_n; ++k)
                search_idx[k] += (latents[k] >= lowers[search_idx[k] + bisect]) ? bisect : 0;
        }
        size_t n_bins = infos_by_lower.size();
        if (n_bins < (size_t{1} << search_size_log))
            for (size_t k = 0; k < batch_n; ++k)
                search_idx[k] = std::min(search_idx[k], n_bins - 1);
    }

    /// Dissects the whole (single-page) latent array. `latents` is in original order.
    DissectedVar<L> dissect(const PcoArray<L> & latents) const
    {
        DissectedVar<L> d;
        d.ans_final_states.fill(encoder.defaultState());
        if (is_trivial)
            return d; // empty body

        size_t n = latents.size();
        d.offsets.resize(n);
        d.offset_bits.resize(n);

        if (!needs_ans)
        {
            // Single bin: no ANS, constant lower and offset_bits — skip the per-value bin search.
            L lower = infos_by_lower[0].lower;
            Bitlen ob = infos_by_lower[0].offset_bits;
            for (size_t i = 0; i < n; ++i)
            {
                d.offsets[i] = static_cast<L>(latents[i] - lower);
                d.offset_bits[i] = ob;
            }
            return d;
        }

        d.ans_vals.resize(n);
        d.ans_bits.resize(n);
        std::array<Symbol, FULL_BATCH_N> symbols{};
        std::array<size_t, FULL_BATCH_N> search_idx{};
        size_t n_batches = (n + FULL_BATCH_N - 1) / FULL_BATCH_N;
        for (size_t batch_idx = n_batches; batch_idx-- > 0;)
        {
            size_t start = batch_idx * FULL_BATCH_N;
            size_t end = std::min(start + FULL_BATCH_N, n);
            size_t batch_n = end - start;

            searchBatch(latents.data() + start, batch_n, search_idx.data());
            for (size_t k = 0; k < batch_n; ++k)
            {
                const BinCompressionInfo<L> & info = infos_by_lower[search_idx[k]];
                symbols[k] = info.symbol;
                d.offset_bits[start + k] = info.offset_bits;
                d.offsets[start + k] = static_cast<L>(latents[start + k] - info.lower);
            }

            encodeAnsInReverse(&d.ans_vals[start], &d.ans_bits[start], d.ans_final_states, symbols.data(), batch_n);
        }
        return d;
    }

    /// Writes one batch of this latent var's page body (ANS values then offsets) starting at
    /// `batch_start` into this var's own (possibly shorter) stored sequence. Writes nothing if the
    /// batch is past this var's end (matches chunk_latent_compressor.rs::write_dissected_batch).
    void writeBatch(const DissectedVar<L> & d, size_t batch_start, BitWriter & writer) const
    {
        size_t len = d.offsets.size();
        if (batch_start >= len)
            return;
        size_t end = std::min(batch_start + FULL_BATCH_N, len);
        if (needs_ans)
            for (size_t i = batch_start; i < end; ++i)
                writer.writeUint<AnsState>(d.ans_vals[i], d.ans_bits[i]);
        if (max_u64s_per_offset > 0)
            for (size_t i = batch_start; i < end; ++i)
                writer.writeUint<L>(d.offsets[i], d.offset_bits[i]);
    }

private:
    void encodeAnsInReverse(AnsState * ans_vals, Bitlen * ans_bits, std::array<AnsState, ANS_INTERLEAVING> & final_states,
                            const Symbol * symbols, size_t batch_n) const
    {
        if (encoder.size_log == 0)
        {
            for (size_t i = 0; i < batch_n; ++i)
                ans_bits[i] = 0;
            return;
        }

        size_t final_base_i = (batch_n / ANS_INTERLEAVING) * ANS_INTERLEAVING;
        size_t final_j = batch_n % ANS_INTERLEAVING;

        auto step = [&](size_t i, size_t j)
        {
            auto [new_state, bitlen] = encoder.encode(final_states[j], symbols[i]);
            ans_vals[i] = lowestBitsFast<AnsState>(final_states[j], bitlen);
            ans_bits[i] = bitlen;
            final_states[j] = new_state;
        };

        for (size_t j = final_j; j-- > 0;)
            step(final_base_i + j, j);
        for (size_t base_i = final_base_i; base_i > 0;)
        {
            base_i -= ANS_INTERLEAVING;
            for (size_t j = ANS_INTERLEAVING; j-- > 0;)
                step(base_i + j, j);
        }
    }
};

}
