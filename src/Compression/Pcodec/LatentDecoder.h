#pragma once

#include <Compression/Pcodec/Ans.h>
#include <Compression/Pcodec/BitReader.h>
#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/Metadata.h>
#include <Compression/Pcodec/PcodecError.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <vector>

/// When built inside ClickHouse, CompressionCodecPco.cpp defines PCODEC_MULTITARGET so the hot
/// decode loops get AVX2 (x86-64-v3) and AVX-512 (x86-64-v4) variants with runtime dispatch.
/// Standalone unit tests leave it undefined and use the portable scalar path.
#ifdef PCODEC_MULTITARGET
#    include <Common/TargetSpecific.h>
#else
#    define MULTITARGET_FUNCTION_HEADER(...) __VA_ARGS__
#    define MULTITARGET_FUNCTION_BODY(...) __VA_ARGS__
#    define MULTITARGET_FUNCTION_X86_V4_V3(FUNCTION_HEADER, name, FUNCTION_BODY) FUNCTION_HEADER name FUNCTION_BODY
#endif

/** Per-latent-variable decoder: ANS symbol decode -> offset unpacking -> delta inversion.
  *
  * Ported from tmp/pcodec_ref/pco/src/{page_latent_decompressor,chunk_latent_decompressor}.rs and
  * the delta modules. Templated on the latent width L (uint8/16/32/64). Delta "lookback" latents
  * are always uint32_t (`DeltaLookback`).
  */
namespace DB::Pcodec
{

/// Widened signed accumulator type for Conv1 (latent_priv.rs: Conv). u64 conv1 is unsupported.
template <Latent L> struct ConvTypeFor;
template <> struct ConvTypeFor<uint8_t> { using type = int16_t; };
template <> struct ConvTypeFor<uint16_t> { using type = int32_t; };
template <> struct ConvTypeFor<uint32_t> { using type = int64_t; };
template <> struct ConvTypeFor<uint64_t> { using type = int64_t; }; // unused (conv1 rejected for 64-bit)

/// --- Performance-critical decode loops, multi-versioned for runtime CPU dispatch. ---
/// These are free functions (not class members) so the MULTITARGET macro can emit per-arch
/// specializations. They are `static` (internal linkage) to avoid ODR issues across translation
/// units. Mirrors page_latent_decompressor.rs::{read_offsets, read_full_ans_symbols}.

/// Offset unpacking: latents[i] += read offset_bits[i] bits at offset_bits_csum[i]. The reference
/// specifically vectorizes this loop; `read_bytes` (4/8/15) is the specialized access width.
MULTITARGET_FUNCTION_X86_V4_V3(
    MULTITARGET_FUNCTION_HEADER(template <Latent L, size_t read_bytes> static void),
    pcoReadOffsetsImpl,
    MULTITARGET_FUNCTION_BODY((const uint8_t * src, size_t base_bit_idx, const Bitlen * offset_bits_csum,
                              const Bitlen * offset_bits, L * latents, size_t n) /// NOLINT
    {
        for (size_t i = 0; i < n; ++i)
        {
            size_t bit_idx = base_bit_idx + offset_bits_csum[i];
            L offset = readUintAt<L, read_bytes>(src, bit_idx / 8, static_cast<Bitlen>(bit_idx % 8), offset_bits[i]);
            latents[i] = static_cast<L>(latents[i] + offset);
        }
    })
)

/// The 4-way interleaved tANS decode loop. State indices are kept in 4 separate locals (not an
/// array) so the compiler keeps them in registers, as the reference does.
MULTITARGET_FUNCTION_X86_V4_V3(
    MULTITARGET_FUNCTION_HEADER(template <Latent L> static void),
    pcoReadFullAnsSymbolsImpl,
    MULTITARGET_FUNCTION_BODY((const uint8_t * src, size_t * stale_io, Bitlen * bpb_io, const AnsNode * nodes,
                              const L * lowers, Bitlen * offset_bits_csum, Bitlen * offset_bits, L * latents,
                              AnsState * states) /// NOLINT
    {
        size_t stale = *stale_io;
        Bitlen bpb = *bpb_io;
        Bitlen offset_bit_idx = 0;
        AnsState s0 = states[0];
        AnsState s1 = states[1];
        AnsState s2 = states[2];
        AnsState s3 = states[3];
        for (size_t base_i = 0; base_i < FULL_BATCH_N; base_i += ANS_INTERLEAVING)
        {
            stale += bpb / 8;
            bpb %= 8;
            uint64_t packed = loadU64LE(src + stale);
            {
                const AnsNode & node = nodes[s0]; Bitlen btr = node.bits_to_read;
                AnsState av = static_cast<AnsState>(packed >> bpb) & ((AnsState{1} << btr) - 1);
                offset_bits_csum[base_i] = offset_bit_idx; offset_bits[base_i] = node.offset_bits; latents[base_i] = lowers[s0];
                bpb += btr; offset_bit_idx += node.offset_bits; s0 = static_cast<AnsState>(node.next_state_idx_base) + av;
            }
            {
                const AnsNode & node = nodes[s1]; Bitlen btr = node.bits_to_read;
                AnsState av = static_cast<AnsState>(packed >> bpb) & ((AnsState{1} << btr) - 1);
                offset_bits_csum[base_i + 1] = offset_bit_idx; offset_bits[base_i + 1] = node.offset_bits; latents[base_i + 1] = lowers[s1];
                bpb += btr; offset_bit_idx += node.offset_bits; s1 = static_cast<AnsState>(node.next_state_idx_base) + av;
            }
            {
                const AnsNode & node = nodes[s2]; Bitlen btr = node.bits_to_read;
                AnsState av = static_cast<AnsState>(packed >> bpb) & ((AnsState{1} << btr) - 1);
                offset_bits_csum[base_i + 2] = offset_bit_idx; offset_bits[base_i + 2] = node.offset_bits; latents[base_i + 2] = lowers[s2];
                bpb += btr; offset_bit_idx += node.offset_bits; s2 = static_cast<AnsState>(node.next_state_idx_base) + av;
            }
            {
                const AnsNode & node = nodes[s3]; Bitlen btr = node.bits_to_read;
                AnsState av = static_cast<AnsState>(packed >> bpb) & ((AnsState{1} << btr) - 1);
                offset_bits_csum[base_i + 3] = offset_bit_idx; offset_bits[base_i + 3] = node.offset_bits; latents[base_i + 3] = lowers[s3];
                bpb += btr; offset_bit_idx += node.offset_bits; s3 = static_cast<AnsState>(node.next_state_idx_base) + av;
            }
        }
        *stale_io = stale;
        *bpb_io = bpb;
        states[0] = s0; states[1] = s1; states[2] = s2; states[3] = s3;
    })
)

/// Runtime dispatch to the best available variant. Returns the total offset bits consumed.
template <Latent L, size_t read_bytes>
inline void dispatchReadOffsets(const uint8_t * src, size_t base, const Bitlen * csum, const Bitlen * obits, L * latents, size_t n)
{
#ifdef PCODEC_MULTITARGET
#    if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        pcoReadOffsetsImpl_x86_64_v4<L, read_bytes>(src, base, csum, obits, latents, n);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        pcoReadOffsetsImpl_x86_64_v3<L, read_bytes>(src, base, csum, obits, latents, n);
        return;
    }
#    endif
#endif
    pcoReadOffsetsImpl<L, read_bytes>(src, base, csum, obits, latents, n);
}

template <Latent L>
inline void dispatchReadFullAnsSymbols(const uint8_t * src, size_t * stale, Bitlen * bpb, const AnsNode * nodes,
                                       const L * lowers, Bitlen * csum, Bitlen * obits, L * latents, AnsState * states)
{
#ifdef PCODEC_MULTITARGET
#    if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        pcoReadFullAnsSymbolsImpl_x86_64_v4<L>(src, stale, bpb, nodes, lowers, csum, obits, latents, states);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        pcoReadFullAnsSymbolsImpl_x86_64_v3<L>(src, stale, bpb, nodes, lowers, csum, obits, latents, states);
        return;
    }
#    endif
#endif
    pcoReadFullAnsSymbolsImpl<L>(src, stale, bpb, nodes, lowers, csum, obits, latents, states);
}

template <Latent L>
class LatentVarDecoder
{
public:
    // --- chunk-level (immutable after initChunk) ---
    LatentVarDeltaEncoding delta_encoding;
    size_t bytes_per_offset = 0;
    std::vector<L> state_lowers;
    size_t n_bins = 0;
    AnsDecoder decoder;

    // --- scratch (reused per batch) ---
    std::array<Bitlen, FULL_BATCH_N> offset_bits{};
    std::array<Bitlen, FULL_BATCH_N> offset_bits_csum{}; // prefix sum of offset_bits; keeps read_offsets parallel
    std::array<L, FULL_BATCH_N> latents{};

    // --- page-level state ---
    std::array<AnsState, ANS_INTERLEAVING> ans_state_idxs{};
    std::vector<L> delta_state;
    size_t delta_state_pos = 0;

    void initChunk(const ChunkLatentVarMeta & var, const LatentVarDeltaEncoding & de)
    {
        delta_encoding = de;

        Bitlen max_ob = 0;
        std::vector<Bitlen> bin_offset_bits(var.bins.size());
        std::vector<Weight> weights(var.bins.size());
        for (size_t i = 0; i < var.bins.size(); ++i)
        {
            bin_offset_bits[i] = var.bins[i].offset_bits;
            weights[i] = var.bins[i].weight;
            max_ob = std::max(max_ob, var.bins[i].offset_bits);
        }
        bytes_per_offset = calcMaxBytes(max_ob);
        n_bins = var.bins.size();

        AnsSpec spec = AnsSpec::fromWeights(var.ans_size_log, weights);
        state_lowers.resize(spec.state_symbols.size());
        for (size_t i = 0; i < spec.state_symbols.size(); ++i)
        {
            Symbol s = spec.state_symbols[i];
            state_lowers[i] = s < var.bins.size() ? static_cast<L>(var.bins[s].lower) : L{0};
        }
        decoder = AnsDecoder::make(spec, bin_offset_bits);

        // Single-bin fast path: state never changes, so prefill scratch once.
        if (n_bins == 1)
        {
            Bitlen csum = 0;
            for (size_t i = 0; i < FULL_BATCH_N; ++i)
            {
                offset_bits[i] = var.bins[0].offset_bits;
                offset_bits_csum[i] = csum;
                latents[i] = static_cast<L>(var.bins[0].lower);
                csum += var.bins[0].offset_bits;
            }
        }
    }

    void initPage(const std::array<AnsState, ANS_INTERLEAVING> & finals, std::vector<L> stored_delta_state)
    {
        ans_state_idxs = finals;
        if (delta_encoding.variant == DeltaEncodingVariant::Lookback)
        {
            size_t window_n = delta_encoding.lookback.windowN();
            size_t buffer_n = std::max(window_n, FULL_BATCH_N) * 2;
            delta_state.assign(buffer_n, L{0});
            std::copy(stored_delta_state.begin(), stored_delta_state.end(), delta_state.begin() + (window_n - stored_delta_state.size()));
            delta_state_pos = window_n;
        }
        else
        {
            delta_state = std::move(stored_delta_state);
            delta_state_pos = 0;
        }
    }

    /// Reads `batch_n` latents into scratch (ANS symbols then offsets), before delta inversion.
    void readBatchPreDelta(BitReader & reader, size_t batch_n)
    {
        if (batch_n == 0)
            return;

        if (n_bins > 1)
        {
            if (batch_n == FULL_BATCH_N)
                readFullAnsSymbols(reader);
            else
                readAnsSymbols(reader, batch_n);
        }
        // else: single-bin fast path — latents/offset_bits/csum were prefilled in initChunk.
        else
        {
            std::fill_n(latents.begin(), batch_n, state_lowers[0]);
        }

        switch (bytes_per_offset == 0 ? 0 : (bytes_per_offset <= 4 ? (latentBits<L> <= 32 ? 4 : 8) : (bytes_per_offset <= 8 ? 8 : 15)))
        {
            case 0:
                break;
            case 4:
                readOffsets<4>(reader, batch_n);
                break;
            case 8:
                readOffsets<8>(reader, batch_n);
                break;
            default:
                readOffsets<15>(reader, batch_n);
                break;
        }
    }

    /// Full batch read + delta inversion. `lookbacks` is the decoded delta latent variable (only
    /// used by Lookback), of length `lookbacks_len`.
    void readBatch(BitReader & reader, const uint32_t * lookbacks, size_t lookbacks_len, size_t n_remaining)
    {
        size_t n_per_state = delta_encoding.nLatentsPerState();
        size_t n_remaining_pre_delta = n_remaining > n_per_state ? n_remaining - n_per_state : 0;
        size_t pre_delta_len = std::min<size_t>(FULL_BATCH_N, n_remaining_pre_delta);
        readBatchPreDelta(reader, pre_delta_len);
        size_t dst_len = std::min<size_t>(n_remaining, FULL_BATCH_N);
        decodeDelta(lookbacks, lookbacks_len, dst_len);
    }

private:
    void readFullAnsSymbols(BitReader & reader)
    {
        dispatchReadFullAnsSymbols<L>(
            reader.src, &reader.stale_byte_idx, &reader.bits_past_byte, decoder.nodes.data(), state_lowers.data(),
            offset_bits_csum.data(), offset_bits.data(), latents.data(), ans_state_idxs.data());
    }

    void readAnsSymbols(BitReader & reader, size_t batch_n)
    {
        const uint8_t * src = reader.src;
        size_t stale = reader.stale_byte_idx;
        Bitlen bpb = reader.bits_past_byte;
        Bitlen offset_bit_idx = 0;
        std::array<AnsState, ANS_INTERLEAVING> states = ans_state_idxs;
        const AnsNode * nodes = decoder.nodes.data();
        const L * lowers = state_lowers.data();

        for (size_t i = 0; i < batch_n; ++i)
        {
            size_t j = i % ANS_INTERLEAVING;
            AnsState state_idx = states[j];
            stale += bpb / 8;
            bpb %= 8;
            uint64_t packed = loadU64LE(src + stale);
            const AnsNode & node = nodes[state_idx];
            Bitlen bits_to_read = node.bits_to_read;
            auto ans_val = static_cast<AnsState>(packed >> bpb) & ((AnsState{1} << bits_to_read) - 1);
            offset_bits_csum[i] = offset_bit_idx;
            offset_bits[i] = node.offset_bits;
            latents[i] = lowers[state_idx];
            bpb += bits_to_read;
            offset_bit_idx += node.offset_bits;
            states[j] = static_cast<AnsState>(node.next_state_idx_base) + ans_val;
        }

        reader.stale_byte_idx = stale;
        reader.bits_past_byte = bpb;
        ans_state_idxs = states;
    }

    template <size_t read_bytes>
    void readOffsets(BitReader & reader, size_t n)
    {
        size_t base_bit_idx = reader.bitIdx();
        dispatchReadOffsets<L, read_bytes>(reader.src, base_bit_idx, offset_bits_csum.data(), offset_bits.data(), latents.data(), n);
        size_t final_bit_idx = base_bit_idx + offset_bits_csum[n - 1] + offset_bits[n - 1];
        reader.stale_byte_idx = final_bit_idx / 8;
        reader.bits_past_byte = static_cast<Bitlen>(final_bit_idx % 8);
    }

    static void toggleCenterInPlace(L * data, size_t len)
    {
        for (size_t i = 0; i < len; ++i)
            data[i] = toggleCenter(data[i]);
    }

    void decodeDelta(const uint32_t * lookbacks, size_t lookbacks_len, size_t len)
    {
        switch (delta_encoding.variant)
        {
            case DeltaEncodingVariant::None:
                return;
            case DeltaEncodingVariant::Consecutive:
            {
                toggleCenterInPlace(latents.data(), len);
                for (size_t mi = delta_state.size(); mi-- > 0;)
                {
                    L moment = delta_state[mi];
                    for (size_t i = 0; i < len; ++i)
                    {
                        L tmp = latents[i];
                        latents[i] = moment;
                        moment = static_cast<L>(moment + tmp);
                    }
                    delta_state[mi] = moment;
                }
                return;
            }
            case DeltaEncodingVariant::Lookback:
                decodeLookback(lookbacks, lookbacks_len, len);
                return;
            case DeltaEncodingVariant::Conv1:
                decodeConv1(len);
                return;
        }
    }

    void decodeLookback(const uint32_t * lookbacks, size_t lookbacks_len, size_t len)
    {
        toggleCenterInPlace(latents.data(), len);
        size_t window_n = delta_encoding.lookback.windowN();
        size_t state_n = delta_encoding.lookback.stateN();
        size_t start_pos = delta_state_pos;
        size_t batch_n = len;
        if (start_pos + batch_n > delta_state.size())
        {
            std::copy(delta_state.begin() + (start_pos - window_n), delta_state.begin() + start_pos, delta_state.begin());
            start_pos = window_n;
        }
        bool oob = false;
        size_t paired = std::min(len, lookbacks_len);
        for (size_t i = 0; i < paired; ++i)
        {
            size_t pos = start_pos + i;
            size_t lb = 0;
            if (lookbacks[i] <= static_cast<uint32_t>(window_n))
                lb = lookbacks[i];
            else
            {
                oob = true;
                lb = 1;
            }
            delta_state[pos] = static_cast<L>(latents[i] + delta_state[pos - lb]);
        }
        size_t end_pos = start_pos + batch_n;
        for (size_t i = 0; i < batch_n; ++i)
            latents[i] = delta_state[start_pos - state_n + i];
        delta_state_pos = end_pos;
        if (oob)
            throw PcodecError("pcodec: delta lookback exceeded window n");
    }

    void decodeConv1(size_t len)
    {
        using Conv = typename ConvTypeFor<L>::type;
        toggleCenterInPlace(latents.data(), len);
        size_t order = delta_encoding.conv1.weights.size();
        std::vector<Conv> weights(order);
        for (size_t i = 0; i < order; ++i)
            weights[i] = static_cast<Conv>(delta_encoding.conv1.weights[i]);
        Conv bias = static_cast<Conv>(delta_encoding.conv1.bias);
        Bitlen quantization = delta_encoding.conv1.quantization;

        std::vector<L> residuals(len + order);
        std::copy(delta_state.begin(), delta_state.begin() + order, residuals.begin());
        std::copy(latents.begin(), latents.begin() + len, residuals.begin() + order);
        for (size_t i = order; i < residuals.size(); ++i)
        {
            Conv s = bias;
            for (size_t j = 0; j < order; ++j)
                s += weights[j] * static_cast<Conv>(residuals[i - order + j]);
            Conv clamped = s > Conv{0} ? s : Conv{0};
            L pred = static_cast<L>(clamped >> quantization);
            residuals[i] = static_cast<L>(residuals[i] + pred);
        }
        std::copy(residuals.begin(), residuals.begin() + len, latents.begin());
        std::copy(residuals.begin() + len, residuals.begin() + len + order, delta_state.begin());
    }
};

}
