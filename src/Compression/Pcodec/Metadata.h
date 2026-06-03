#pragma once

#include <Compression/Pcodec/PcoArray.h>

#include <Compression/Pcodec/BitReader.h>
#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/NumberTraits.h>
#include <Compression/Pcodec/PcodecError.h>

#include <cstdint>
#include <optional>
#include <vector>

/** Chunk- and page-level metadata structures and their parsers, ported from
  * the tmp/pcodec_ref/pco/src/metadata modules.
  *
  * Metadata field values do not depend on the bit-read width (the reader always masks to the
  * requested number of bits), so all reads here go through `BitReader::readU64`. Only the latent
  * decode hot path (LatentVarDecoder) is templated on the actual latent width.
  */
namespace DB::Pcodec
{

/// A bin: a latent is reconstructed as `lower + offset`, offset in [0, 2^offset_bits).
struct Bin
{
    Weight weight = 0;
    uint64_t lower = 0;
    Bitlen offset_bits = 0;
};

struct DeltaLookbackConfig
{
    Bitlen state_n_log = 0;
    Bitlen window_n_log = 0;

    size_t stateN() const { return size_t{1} << state_n_log; }
    size_t windowN() const { return size_t{1} << window_n_log; }
};

struct DeltaConv1Config
{
    Bitlen quantization = 0;
    int64_t bias = 0;
    PcoArray<int64_t> weights;
};

/// How a chunk applied delta encoding (chunk-level). Per-latent-var encoding is derived from this.
struct DeltaEncoding
{
    DeltaEncodingVariant variant = DeltaEncodingVariant::None;
    size_t consecutive_order = 0;
    bool secondary_uses_delta = false;
    DeltaLookbackConfig lookback;
    DeltaConv1Config conv1;
};

/// The delta encoding applied to a single latent variable.
struct LatentVarDeltaEncoding
{
    DeltaEncodingVariant variant = DeltaEncodingVariant::None;
    size_t consecutive_order = 0;
    DeltaLookbackConfig lookback;
    DeltaConv1Config conv1;

    size_t nLatentsPerState() const
    {
        switch (variant)
        {
            case DeltaEncodingVariant::None:
                return 0;
            case DeltaEncodingVariant::Consecutive:
                return consecutive_order;
            case DeltaEncodingVariant::Lookback:
                return lookback.stateN();
            case DeltaEncodingVariant::Conv1:
                return conv1.weights.size();
        }
        return 0;
    }
};

enum class LatentVarKey : uint8_t
{
    Delta,
    Primary,
    Secondary
};

struct Mode
{
    ModeVariant variant = ModeVariant::Classic;
    uint64_t base_latent = 0; // IntMult / FloatMult base (ordered latent)
    Bitlen quant_k = 0; // FloatQuant k
    PcoArray<uint64_t> dict; // Dict entries (ordered latents)
};

struct ChunkLatentVarMeta
{
    Bitlen ans_size_log = 0;
    Bitlen latent_bits = 0; // width of this var's latent type (8/16/32/64)
    PcoArray<Bin> bins;
};

struct ChunkMeta
{
    Mode mode;
    DeltaEncoding delta_encoding;
    std::optional<ChunkLatentVarMeta> delta_var;
    ChunkLatentVarMeta primary_var;
    std::optional<ChunkLatentVarMeta> secondary_var;

    /// Derive the delta encoding applied to a particular latent variable (delta_encoding.rs::for_latent_var).
    LatentVarDeltaEncoding forLatentVar(LatentVarKey key) const
    {
        LatentVarDeltaEncoding res;
        if (delta_encoding.variant == DeltaEncodingVariant::None || key == LatentVarKey::Delta)
            return res; // NoOp (delta vars are never recursively delta-encoded)

        switch (delta_encoding.variant)
        {
            case DeltaEncodingVariant::None:
                return res;
            case DeltaEncodingVariant::Consecutive:
                if (key == LatentVarKey::Primary || delta_encoding.secondary_uses_delta)
                {
                    res.variant = DeltaEncodingVariant::Consecutive;
                    res.consecutive_order = delta_encoding.consecutive_order;
                }
                return res;
            case DeltaEncodingVariant::Lookback:
                if (key == LatentVarKey::Primary || delta_encoding.secondary_uses_delta)
                {
                    res.variant = DeltaEncodingVariant::Lookback;
                    res.lookback = delta_encoding.lookback;
                }
                return res;
            case DeltaEncodingVariant::Conv1:
                if (key == LatentVarKey::Primary)
                {
                    res.variant = DeltaEncodingVariant::Conv1;
                    res.conv1 = delta_encoding.conv1;
                }
                return res;
        }
        return res;
    }
};

/// Reads the mode (mode.rs::read_from). `number_latent_bits` is the width of the number type's latent.
inline Mode readMode(BitReader & reader, uint8_t format_major, Bitlen number_latent_bits)
{
    Mode mode;
    auto variant = static_cast<uint8_t>(reader.readU64(BITS_TO_ENCODE_MODE_VARIANT));
    switch (variant)
    {
        case 0:
            mode.variant = ModeVariant::Classic;
            break;
        case 1:
            if (format_major == 0)
                throw PcodecError("pcodec: cannot decompress data from yanked v0.0.0 with different GCD encoding");
            mode.variant = ModeVariant::IntMult;
            mode.base_latent = reader.readU64(number_latent_bits);
            break;
        case 2:
            mode.variant = ModeVariant::FloatMult;
            mode.base_latent = reader.readU64(number_latent_bits);
            break;
        case 3:
            mode.variant = ModeVariant::FloatQuant;
            mode.quant_k = static_cast<Bitlen>(reader.readU64(BITS_TO_ENCODE_QUANTIZE_K));
            break;
        case 4:
        {
            mode.variant = ModeVariant::Dict;
            size_t n_unique = reader.readU64(BITS_TO_ENCODE_DICT_LEN);
            // byte-align so future implementations might read raw values faster
            reader.drainEmptyByte("pcodec: expected zeros between dict mode length and values");
            // Each dict entry occupies `number_latent_bits` in the stream, so a valid length cannot
            // exceed the remaining bits. Check before resizing to avoid a large up-front allocation
            // driven by a tiny malformed stream.
            if (number_latent_bits != 0 && static_cast<uint64_t>(n_unique) * number_latent_bits > reader.unpadded_bit_size - reader.bitIdx())
                throw PcodecError("pcodec: dict length exceeds remaining stream size");
            mode.dict.resize(n_unique);
            for (size_t i = 0; i < n_unique; ++i)
                mode.dict[i] = reader.readU64(number_latent_bits);
            break;
        }
        default:
            throw PcodecError("pcodec: unknown mode variant");
    }
    return mode;
}

/// Reads the delta encoding (delta_encoding.rs::read_from). Only format major >= 3 is supported here.
inline DeltaEncoding readDeltaEncoding(BitReader & reader, uint8_t format_major)
{
    DeltaEncoding de;
    if (format_major < 3)
    {
        // pre-v3: single order field
        size_t order = reader.readU64(BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
        if (order == 0)
            de.variant = DeltaEncodingVariant::None;
        else
        {
            de.variant = DeltaEncodingVariant::Consecutive;
            de.consecutive_order = order;
        }
        return de;
    }

    auto variant = static_cast<uint8_t>(reader.readU64(BITS_TO_ENCODE_DELTA_ENCODING_VARIANT));
    switch (variant)
    {
        case 0:
            de.variant = DeltaEncodingVariant::None;
            break;
        case 1:
        {
            size_t order = reader.readU64(BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
            if (order == 0)
                throw PcodecError("pcodec: Consecutive delta encoding order must not be 0");
            de.variant = DeltaEncodingVariant::Consecutive;
            de.consecutive_order = order;
            de.secondary_uses_delta = reader.readBool();
            break;
        }
        case 2:
        {
            Bitlen window_n_log = 1 + static_cast<Bitlen>(reader.readU64(BITS_TO_ENCODE_DELTA_LOOKBACK_WINDOW_N_LOG));
            Bitlen state_n_log = static_cast<Bitlen>(reader.readU64(BITS_TO_ENCODE_DELTA_LOOKBACK_STATE_N_LOG));
            if (state_n_log > window_n_log)
                throw PcodecError("pcodec: lookback state size log exceeded window size log");
            de.variant = DeltaEncodingVariant::Lookback;
            de.lookback.window_n_log = window_n_log;
            de.lookback.state_n_log = state_n_log;
            de.secondary_uses_delta = reader.readBool();
            break;
        }
        case 3:
        {
            de.variant = DeltaEncodingVariant::Conv1;
            de.conv1.quantization = static_cast<Bitlen>(reader.readU64(BITS_TO_ENCODE_DELTA_CONV_QUANTIZATION));
            de.conv1.bias = NumberTraits<int64_t>::fromLatentOrdered(reader.readU64(64));
            size_t order = 1 + reader.readU64(BITS_TO_ENCODE_DELTA_CONV_N_WEIGHTS);
            de.conv1.weights.resize(order);
            for (size_t i = 0; i < order; ++i)
                de.conv1.weights[i] = NumberTraits<int32_t>::fromLatentOrdered(static_cast<uint32_t>(reader.readU64(32)));
            break;
        }
        default:
            throw PcodecError("pcodec: unknown delta encoding variant");
    }
    return de;
}

/// Reads one latent-variable's metadata (chunk_latent_var.rs::read_from).
inline ChunkLatentVarMeta readChunkLatentVarMeta(BitReader & reader, Bitlen latent_bits)
{
    ChunkLatentVarMeta var;
    var.latent_bits = latent_bits;
    var.ans_size_log = static_cast<Bitlen>(reader.readU64(BITS_TO_ENCODE_ANS_SIZE_LOG));
    size_t n_bins = reader.readU64(BITS_TO_ENCODE_N_BINS);

    if ((size_t{1} << var.ans_size_log) < n_bins)
        throw PcodecError("pcodec: ANS size log too small for number of bins");
    if (n_bins == 1 && var.ans_size_log > 0)
        throw PcodecError("pcodec: only 1 bin but ANS size log is nonzero");
    if (var.ans_size_log > MAX_ANS_BITS)
        throw PcodecError("pcodec: ANS size log exceeds maximum");

    Bitlen offset_bits_bits = static_cast<Bitlen>(32 - std::countl_zero(latent_bits));
    var.bins.resize(n_bins);
    for (Bin & bin : var.bins)
    {
        bin.weight = static_cast<Weight>(reader.readU64(var.ans_size_log)) + 1;
        bin.lower = reader.readU64(latent_bits);
        bin.offset_bits = static_cast<Bitlen>(reader.readU64(offset_bits_bits));
        if (bin.offset_bits > latent_bits)
        {
            reader.checkInBounds();
            throw PcodecError("pcodec: bin offset bits exceeds latent type width");
        }
    }
    return var;
}

}
