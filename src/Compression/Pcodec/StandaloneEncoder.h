#pragma once

#include <Compression/Pcodec/PcoArray.h>

#include <Compression/Pcodec/Binning.h>
#include <Compression/Pcodec/BitWriter.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/DeltaEncode.h>
#include <Compression/Pcodec/LatentEncoder.h>
#include <Compression/Pcodec/Metadata.h>
#include <Compression/Pcodec/Modes.h>
#include <Compression/Pcodec/NumberTraits.h>
#include <Compression/Pcodec/StandaloneDecoder.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

/** Top-level encoder producing a standalone `.pco` stream. Ported from
  * tmp/pcodec_ref/pco/src/standalone/compressor.rs + wrapped/{file,chunk}_compressor.rs.
  *
  * Produces a single chunk / single page. It auto-selects a mode (Classic / IntMult / FloatQuant;
  * a detected mode is only used if it beats Classic's estimated size) and a consecutive delta
  * order, then does full binning + bin-optimization + tANS. The output is bit-compatible with the
  * reference decoder. (FloatMult auto-detection and Lookback/Conv1 delta on the encode side are
  * not yet ported; the decoder already supports them.)
  */
namespace DB::Pcodec
{

inline Bitlen chooseUnoptimizedBinsLog(size_t compression_level, size_t n)
{
    auto level = static_cast<Bitlen>(compression_level);
    Bitlen log_n = n == 0 ? 0 : static_cast<Bitlen>(63 - std::countl_zero(static_cast<uint64_t>(n)));
    Bitlen fast = log_n > 4 ? log_n - 4 : 0;
    if (level <= fast)
        return level;
    return fast + (level - fast) / 2;
}

inline void writeVarint(BitWriter & writer, uint64_t n)
{
    Bitlen power = n == 0 ? 1 : static_cast<Bitlen>(64 - std::countl_zero(n));
    writer.writeU64(power - 1, BITS_TO_ENCODE_VARINT_POWER);
    writer.writeU64(n, power);
}

inline void writeChunkLatentVarMeta(BitWriter & writer, Bitlen ans_size_log, const PcoArray<Bin> & bins, Bitlen latent_bits)
{
    writer.writeU64(ans_size_log, BITS_TO_ENCODE_ANS_SIZE_LOG);
    writer.writeU64(bins.size(), BITS_TO_ENCODE_N_BINS);
    Bitlen offset_bits_bits = static_cast<Bitlen>(32 - std::countl_zero(latent_bits));
    for (const Bin & bin : bins)
    {
        writer.writeU64(bin.weight - 1, ans_size_log);
        writer.writeU64(bin.lower, latent_bits);
        writer.writeU64(bin.offset_bits, offset_bits_bits);
    }
}

/// Upper bound on the standalone stream size: up to two latent variables (primary + secondary),
/// each up to L::BITS + MAX_ANS_BITS per value, plus framing and slack for the BitWriter.
template <typename T>
inline size_t encodeStandaloneMaxSize(size_t n)
{
    using L = typename NumberTraits<T>::Latent;
    return 256 + 2 * n * (sizeof(L) + MAX_ANS_BYTES + 2) + (size_t{1} << 17) + 64;
}

/// Encodes `n` values of type T (read from `src_bytes` via memcpy, so it need not be aligned to
/// alignof(T)) as a standalone `.pco` stream, written directly into `out` (which must have at
/// least `encodeStandaloneMaxSize<T>(n)` bytes). The buffer need NOT be zero-initialized. Returns
/// the number of bytes written.
template <typename T>
/// `out` is an output buffer written through BitWriter; const would break the BitWriter ctor.
/// NOLINTNEXTLINE(readability-non-const-parameter)
size_t encodeStandaloneInto(const uint8_t * src_bytes, size_t n, uint8_t * out, size_t compression_level = DEFAULT_COMPRESSION_LEVEL)
{
    using L = typename NumberTraits<T>::Latent;
    constexpr Bitlen l_bits = latentBits<L>;
    auto type_byte = static_cast<uint8_t>(NumberTraits<T>::type_byte);

    BitWriter writer(out, encodeStandaloneMaxSize<T>(n));

    // --- standalone header ---
    writer.writeAlignedBytes(MAGIC_HEADER.data(), MAGIC_HEADER.size());
    writer.writeU64(CURRENT_STANDALONE_VERSION, BITS_TO_ENCODE_STANDALONE_VERSION);
    uint8_t uniform = type_byte;
    writer.writeAlignedBytes(&uniform, 1);
    writeVarint(writer, n);
    writer.finishByte();

    // --- wrapped header (format version 4.1) ---
    uint8_t version_bytes[2] = {4, 1};
    writer.writeAlignedBytes(version_bytes, 2);

    if (n > 0)
    {
        // --- chunk preamble ---
        writer.writeAlignedBytes(&type_byte, 1);
        writer.writeU64(n - 1, BITS_TO_ENCODE_N_ENTRIES);

        // --- read values, detect a candidate mode (cheap, sample-based) ---
        PcoArray<T> vals(n);
        std::memcpy(vals.data(), src_bytes, n * sizeof(T));
        ModeInfo info = detectMode<T>(vals);

        Bitlen unoptimized_bins_log = chooseUnoptimizedBinsLog(compression_level, n);
        Bitlen sec_ubl = std::min<Bitlen>(unoptimized_bins_log, LIMITED_UNOPTIMIZED_BINS_LOG);

        auto make_bins = [](const TrainedBins<L> & t)
        {
            PcoArray<Bin> bins(t.infos.size());
            for (size_t i = 0; i < t.infos.size(); ++i)
                bins[i] = Bin{t.infos[i].weight, static_cast<uint64_t>(t.infos[i].lower), t.infos[i].offset_bits};
            return bins;
        };

        // Classic baseline latents (cheap), reused if Classic wins.
        PcoArray<L> primary_latents(n);
        for (size_t i = 0; i < n; ++i)
            primary_latents[i] = NumberTraits<T>::toLatentOrdered(vals[i]);

        // Decide the mode on cheap sample-based estimates (a detected mode is used only if it beats
        // Classic, so the ratio never regresses). Crucially, the expensive full split (2M divisions
        // for IntMult) is done ONLY for the winner — not for a detected-then-rejected mode.
        ModeVariant variant = ModeVariant::Classic;
        uint64_t base_latent = 0;
        Bitlen quant_k = 0;
        bool has_secondary = false;
        PcoArray<L> secondary;

        if (info.variant != ModeVariant::Classic)
        {
            double classic_cost = estimateCostPerNum(primary_latents, unoptimized_bins_log, /*allow_delta=*/true);
            // Split only a small sample for the mode estimate.
            PcoArray<T> sample_vals = spreadSample(vals);
            PcoArray<L> sample_primary;
            PcoArray<L> sample_secondary;
            splitForMode<T>(sample_vals.data(), sample_vals.size(), info, sample_primary, sample_secondary);
            double mode_cost = estimateCostPerNum(sample_primary, unoptimized_bins_log, /*allow_delta=*/true);
            if (info.has_secondary)
                mode_cost += estimateCostPerNum(sample_secondary, sec_ubl, /*allow_delta=*/false);

            if (mode_cost < classic_cost)
            {
                variant = info.variant;
                base_latent = info.base_latent;
                quant_k = info.k;
                has_secondary = info.has_secondary;
                // Now do the full split (only for the winner).
                PcoArray<L> mode_primary;
                splitForMode<T>(vals.data(), n, info, mode_primary, secondary);
                primary_latents = std::move(mode_primary);
            }
        }

        // Full encode of the winning plan: one delta-selection + train for the primary, plus one
        // train for the secondary if the mode has one.
        ChosenDelta<L> primary_dp = chooseDelta(std::move(primary_latents), unoptimized_bins_log);
        TrainedBins<L> sec_trained;
        if (has_secondary)
            sec_trained = trainInfos(secondary, sec_ubl);

        PcoArray<Bin> primary_bins = make_bins(primary_dp.trained);
        LatentEncoder<L> primary_enc = LatentEncoder<L>::build(primary_dp.trained, primary_bins);
        DissectedVar<L> primary_dis = primary_enc.dissect(primary_dp.body);

        PcoArray<Bin> sec_bins;
        LatentEncoder<L> sec_enc;
        DissectedVar<L> sec_dis;
        if (has_secondary)
        {
            sec_bins = make_bins(sec_trained);
            sec_enc = LatentEncoder<L>::build(sec_trained, sec_bins);
            sec_dis = sec_enc.dissect(secondary);
        }

        // --- chunk metadata: mode variant + payload, delta encoding, then latent var metas ---
        writer.writeU64(static_cast<uint64_t>(variant), BITS_TO_ENCODE_MODE_VARIANT);
        if (variant == ModeVariant::IntMult)
            writer.writeU64(base_latent, l_bits);
        else if (variant == ModeVariant::FloatQuant)
            writer.writeU64(quant_k, BITS_TO_ENCODE_QUANTIZE_K);

        if (primary_dp.order == 0)
            writer.writeU64(static_cast<uint64_t>(DeltaEncodingVariant::None), BITS_TO_ENCODE_DELTA_ENCODING_VARIANT);
        else
        {
            writer.writeU64(static_cast<uint64_t>(DeltaEncodingVariant::Consecutive), BITS_TO_ENCODE_DELTA_ENCODING_VARIANT);
            writer.writeU64(primary_dp.order, BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
            writer.writeBool(false); // secondary_uses_delta
        }
        writeChunkLatentVarMeta(writer, primary_dp.trained.ans_size_log, primary_bins, l_bits);
        if (has_secondary)
            writeChunkLatentVarMeta(writer, sec_trained.ans_size_log, sec_bins, l_bits);
        writer.finishByte();

        // --- page metadata: primary (delta moments + final ANS states), then secondary (final states) ---
        for (L moment : primary_dp.moments)
            writer.writeU64(moment, l_bits);
        AnsState primary_default = primary_enc.encoder.defaultState();
        for (AnsState s : primary_dis.ans_final_states)
            writer.writeU64(s - primary_default, primary_dp.trained.ans_size_log);
        if (has_secondary)
        {
            AnsState sec_default = sec_enc.encoder.defaultState();
            for (AnsState s : sec_dis.ans_final_states)
                writer.writeU64(s - sec_default, sec_trained.ans_size_log);
        }
        writer.finishByte();

        // --- page body: per batch, primary then secondary ---
        for (size_t batch_start = 0; batch_start < n; batch_start += FULL_BATCH_N)
        {
            primary_enc.writeBatch(primary_dis, batch_start, writer);
            if (has_secondary)
                sec_enc.writeBatch(sec_dis, batch_start, writer);
        }
        writer.finishByte();
    }

    // --- footer ---
    uint8_t term = MAGIC_TERMINATION_BYTE;
    writer.writeAlignedBytes(&term, 1);

    return writer.byteSize();
}

/// Convenience wrapper returning a freshly-allocated vector (used by the standalone API and tests).
/// The scratch buffer is allocated uninitialized — no zeroing is needed by the writer.
template <typename T>
PcoArray<uint8_t> encodeStandalone(const uint8_t * src_bytes, size_t n, size_t compression_level = DEFAULT_COMPRESSION_LEVEL)
{
    size_t cap = encodeStandaloneMaxSize<T>(n);
    auto scratch = std::make_unique_for_overwrite<uint8_t[]>(cap);
    size_t size = encodeStandaloneInto<T>(src_bytes, n, scratch.get(), compression_level);
    return PcoArray<uint8_t>(scratch.get(), scratch.get() + size);
}

}
