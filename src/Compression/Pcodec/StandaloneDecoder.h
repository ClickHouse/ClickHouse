#pragma once

#include <Compression/Pcodec/BitReader.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/LatentDecoder.h>
#include <Compression/Pcodec/Metadata.h>
#include <Compression/Pcodec/NumberTraits.h>
#include <Compression/Pcodec/PcodecError.h>

#include <array>
#include <cstring>
#include <optional>
#include <vector>

/** Top-level decoder for the standalone `.pco` container, ported from
  * tmp/pcodec_ref/pco/src/standalone/decompressor.rs + wrapped/{file,chunk,page}_decompressor.rs.
  *
  * Decodes a whole standalone stream into native-endian values written to `out`.
  * The source buffer MUST have at least `OVERSHOOT_PADDING` readable slack bytes after `src_len`.
  */
namespace DB::Pcodec
{

inline constexpr std::array<uint8_t, 4> MAGIC_HEADER = {112, 99, 111, 33}; // "pco!"
inline constexpr uint8_t MAGIC_TERMINATION_BYTE = 0;
inline constexpr Bitlen BITS_TO_ENCODE_N_ENTRIES = 24;
inline constexpr Bitlen BITS_TO_ENCODE_STANDALONE_VERSION = 8;
inline constexpr Bitlen BITS_TO_ENCODE_VARINT_POWER = 6;
inline constexpr size_t CURRENT_STANDALONE_VERSION = 3;

/// Per-latent-variable page metadata: delta state values (as wide latents) + 4 final ANS states.
struct PageVarMeta
{
    std::vector<uint64_t> delta_state;
    std::array<AnsState, ANS_INTERLEAVING> finals{};
};

inline PageVarMeta readPageVarMeta(BitReader & reader, Bitlen latent_bits, size_t n_latents_per_state, Bitlen ans_size_log)
{
    PageVarMeta meta;
    meta.delta_state.resize(n_latents_per_state);
    for (size_t i = 0; i < n_latents_per_state; ++i)
        meta.delta_state[i] = reader.readU64(latent_bits);
    for (auto & f : meta.finals)
        f = static_cast<AnsState>(reader.readU64(ans_size_log));
    return meta;
}

template <Latent L>
std::vector<L> castDeltaState(const std::vector<uint64_t> & src)
{
    std::vector<L> res(src.size());
    for (size_t i = 0; i < src.size(); ++i)
        res[i] = static_cast<L>(src[i]);
    return res;
}

/// Decodes one chunk's page of `n` values of number type T, writing native-endian values into
/// `out_bytes` (which must be aligned to alignof(T)).
template <typename T>
void decodeChunk(BitReader & reader, const ChunkMeta & meta, size_t n, uint8_t * out_bytes)
{
    using L = typename NumberTraits<T>::Latent;
    constexpr Bitlen l_bits = latentBits<L>;
    // `out_bytes` is required to be aligned to alignof(T) (callers guarantee this); writing through
    // a typed pointer keeps the join loops branch-free and vectorizable.
    T * out = reinterpret_cast<T *>(out_bytes);

    // --- delta latent variable (Lookback only; always u32) ---
    std::optional<LatentVarDecoder<uint32_t>> delta_dec;
    if (meta.delta_var)
    {
        delta_dec.emplace();
        delta_dec->initChunk(*meta.delta_var, meta.forLatentVar(LatentVarKey::Delta));
    }

    size_t n_latents_per_delta_state = meta.forLatentVar(LatentVarKey::Primary).nLatentsPerState();

    // --- read page metadata for every latent var, in order delta -> primary -> secondary ---
    std::optional<PageVarMeta> delta_page;
    if (meta.delta_var)
        delta_page = readPageVarMeta(reader, meta.delta_var->latent_bits, 0, meta.delta_var->ans_size_log);
    PageVarMeta primary_page = readPageVarMeta(
        reader, meta.primary_var.latent_bits, meta.forLatentVar(LatentVarKey::Primary).nLatentsPerState(), meta.primary_var.ans_size_log);
    std::optional<PageVarMeta> secondary_page;
    if (meta.secondary_var)
        secondary_page = readPageVarMeta(
            reader, meta.secondary_var->latent_bits, meta.forLatentVar(LatentVarKey::Secondary).nLatentsPerState(),
            meta.secondary_var->ans_size_log);
    reader.drainEmptyByte("pcodec: non-zero bits at end of data page metadata");

    if (delta_dec)
        delta_dec->initPage(delta_page->finals, castDeltaState<uint32_t>(delta_page->delta_state));

    auto run_batches = [&](auto & primary, auto * secondary, auto join)
    {
        size_t n_remaining = n;
        size_t n_processed = 0;
        while (n_processed < n)
        {
            size_t batch_n = std::min<size_t>(FULL_BATCH_N, n - n_processed);

            const uint32_t * lookbacks = nullptr;
            size_t lookbacks_len = 0;
            if (delta_dec)
            {
                size_t limit = std::min<size_t>(
                    n_remaining > n_latents_per_delta_state ? n_remaining - n_latents_per_delta_state : 0, batch_n);
                delta_dec->readBatchPreDelta(reader, limit);
                lookbacks = delta_dec->latents.data();
                lookbacks_len = limit;
            }

            primary.readBatch(reader, lookbacks, lookbacks_len, n_remaining);
            if (secondary)
                secondary->readBatch(reader, lookbacks, lookbacks_len, n_remaining);

            join(n_processed, batch_n);

            n_remaining -= batch_n;
            n_processed += batch_n;
        }
    };

    if (meta.mode.variant == ModeVariant::Dict)
    {
        LatentVarDecoder<uint32_t> primary;
        primary.initChunk(meta.primary_var, meta.forLatentVar(LatentVarKey::Primary));
        primary.initPage(primary_page.finals, castDeltaState<uint32_t>(primary_page.delta_state));
        const auto & dict = meta.mode.dict;
        run_batches(primary, static_cast<LatentVarDecoder<uint32_t> *>(nullptr), [&](size_t off, size_t bn)
        {
            for (size_t i = 0; i < bn; ++i)
            {
                uint32_t idx = primary.latents[i];
                if (idx >= dict.size())
                    throw PcodecError("pcodec: dict index exceeded dict length");
                out[off + i] = NumberTraits<T>::fromLatentOrdered(static_cast<L>(dict[idx]));
            }
        });
        reader.drainEmptyByte("pcodec: expected trailing bits at end of page to be empty");
        return;
    }

    LatentVarDecoder<L> primary;
    primary.initChunk(meta.primary_var, meta.forLatentVar(LatentVarKey::Primary));
    primary.initPage(primary_page.finals, castDeltaState<L>(primary_page.delta_state));

    std::optional<LatentVarDecoder<L>> secondary;
    if (meta.secondary_var)
    {
        secondary.emplace();
        secondary->initChunk(*meta.secondary_var, meta.forLatentVar(LatentVarKey::Secondary));
        secondary->initPage(secondary_page->finals, castDeltaState<L>(secondary_page->delta_state));
    }
    LatentVarDecoder<L> * sec_ptr = secondary ? &*secondary : nullptr;

    // When the secondary latent variable is a single zero-width bin, every secondary value is the
    // same constant and it contributes nothing to the page body. Skip decoding it entirely and fold
    // the constant into the join — a large win for exactly-quantized / exact-multiple data.
    bool sec_const = sec_ptr && sec_ptr->n_bins <= 1 && sec_ptr->bytes_per_offset == 0;
    L sec_c = (sec_const && !sec_ptr->state_lowers.empty()) ? sec_ptr->state_lowers[0] : L{0};
    LatentVarDecoder<L> * sec_decode = sec_const ? nullptr : sec_ptr;

    const L * pl = primary.latents.data();
    const L * sl = sec_ptr ? sec_ptr->latents.data() : nullptr;

    if (meta.mode.variant == ModeVariant::Classic)
    {
        run_batches(primary, sec_decode, [&](size_t off, size_t bn)
        {
            for (size_t i = 0; i < bn; ++i)
                out[off + i] = NumberTraits<T>::fromLatentOrdered(pl[i]);
        });
    }
    else if constexpr (std::is_integral_v<T>)
    {
        if (meta.mode.variant != ModeVariant::IntMult)
            throw PcodecError("pcodec: unexpected mode for integer type");
        L base = static_cast<L>(meta.mode.base_latent);
        run_batches(primary, sec_decode, [&](size_t off, size_t bn)
        {
            if (sec_const)
                for (size_t i = 0; i < bn; ++i)
                    out[off + i] = NumberTraits<T>::fromLatentOrdered(static_cast<L>(pl[i] * base + sec_c));
            else
                for (size_t i = 0; i < bn; ++i)
                    out[off + i] = NumberTraits<T>::fromLatentOrdered(static_cast<L>(pl[i] * base + sl[i]));
        });
    }
    else // floating point
    {
        if (meta.mode.variant == ModeVariant::FloatMult)
        {
            T base = NumberTraits<T>::fromLatentOrdered(static_cast<L>(meta.mode.base_latent));
            run_batches(primary, sec_decode, [&](size_t off, size_t bn)
            {
                for (size_t i = 0; i < bn; ++i)
                {
                    T unadjusted = NumberTraits<T>::intFloatFromLatent(pl[i]) * base;
                    L adj = sec_const ? sec_c : sl[i];
                    out[off + i] = NumberTraits<T>::fromLatentOrdered(
                        toggleCenter(static_cast<L>(NumberTraits<T>::toLatentOrdered(unadjusted) + adj)));
                }
            });
        }
        else if (meta.mode.variant == ModeVariant::FloatQuant)
        {
            Bitlen k = meta.mode.quant_k;
            L sign_cutoff = static_cast<L>(latentMid<L> >> k);
            L lowest_k_bits_max = static_cast<L>((L{1} << k) - L{1});
            run_batches(primary, sec_decode, [&](size_t off, size_t bn)
            {
                for (size_t i = 0; i < bn; ++i)
                {
                    L y = pl[i];
                    L m = sec_const ? sec_c : sl[i];
                    L low = (y >= sign_cutoff) ? m : static_cast<L>(lowest_k_bits_max - m);
                    out[off + i] = NumberTraits<T>::fromLatentOrdered(static_cast<L>((y << k) + low));
                }
            });
        }
        else
            throw PcodecError("pcodec: unexpected mode for float type");
    }
    reader.drainEmptyByte("pcodec: expected trailing bits at end of page to be empty");
    (void)l_bits;
}

/// Reads the wrapped format version (2 bytes). Returns the major version.
inline uint8_t readWrappedFormatVersion(BitReader & reader)
{
    uint8_t major = reader.readAlignedBytes(1)[0];
    uint8_t minor = major >= 4 ? reader.readAlignedBytes(1)[0] : 0;
    // can_be_decompressed: max supported is 4.1. We can read major <= 4.
    if (major > 4)
        throw PcodecError("pcodec: file format major version is too new to decompress");
    (void)minor;
    return major;
}

/// Element width in bytes for a number-type byte.
inline size_t widthForType(uint8_t type_byte)
{
    switch (static_cast<NumberTypeByte>(type_byte))
    {
        case NumberTypeByte::U8:
        case NumberTypeByte::I8:
            return 1;
        case NumberTypeByte::U16:
        case NumberTypeByte::I16:
        case NumberTypeByte::F16:
            return 2;
        case NumberTypeByte::U32:
        case NumberTypeByte::I32:
        case NumberTypeByte::F32:
            return 4;
        case NumberTypeByte::U64:
        case NumberTypeByte::I64:
        case NumberTypeByte::F64:
            return 8;
    }
    throw PcodecError("pcodec: unknown number type byte");
}

/// Dispatch a chunk by its number-type byte, decoding into `out` (advancing the output pointer by
/// width*n bytes). Returns the element width in bytes.
inline size_t decodeChunkByType(BitReader & reader, uint8_t type_byte, uint8_t format_major, size_t n, uint8_t * out)
{
    auto run = [&]<typename T>() -> size_t
    {
        constexpr Bitlen l_bits = latentBits<typename NumberTraits<T>::Latent>;
        Mode mode = readMode(reader, format_major, l_bits);
        DeltaEncoding de = readDeltaEncoding(reader, format_major);
        ChunkMeta meta;
        meta.mode = std::move(mode);
        meta.delta_encoding = de;
        if (de.variant == DeltaEncodingVariant::Lookback)
            meta.delta_var = readChunkLatentVarMeta(reader, latentBits<uint32_t>);
        Bitlen primary_bits = meta.mode.variant == ModeVariant::Dict ? latentBits<uint32_t> : l_bits;
        meta.primary_var = readChunkLatentVarMeta(reader, primary_bits);
        bool has_secondary = meta.mode.variant == ModeVariant::IntMult || meta.mode.variant == ModeVariant::FloatMult
            || meta.mode.variant == ModeVariant::FloatQuant;
        if (has_secondary)
            meta.secondary_var = readChunkLatentVarMeta(reader, l_bits);
        reader.drainEmptyByte("pcodec: nonzero bits at end of chunk metadata");

        decodeChunk<T>(reader, meta, n, out);
        return sizeof(T);
    };

    switch (static_cast<NumberTypeByte>(type_byte))
    {
        case NumberTypeByte::U32: return run.template operator()<uint32_t>();
        case NumberTypeByte::U64: return run.template operator()<uint64_t>();
        case NumberTypeByte::I32: return run.template operator()<int32_t>();
        case NumberTypeByte::I64: return run.template operator()<int64_t>();
        case NumberTypeByte::F32: return run.template operator()<float>();
        case NumberTypeByte::F64: return run.template operator()<double>();
        case NumberTypeByte::U16: return run.template operator()<uint16_t>();
        case NumberTypeByte::I16: return run.template operator()<int16_t>();
        case NumberTypeByte::U8: return run.template operator()<uint8_t>();
        case NumberTypeByte::I8: return run.template operator()<int8_t>();
        case NumberTypeByte::F16:
            throw PcodecError("pcodec: 16-bit float (f16) is not supported");
    }
    throw PcodecError("pcodec: unknown number type byte");
}

/// Decodes a whole standalone `.pco` stream into `out`. Returns the number of bytes written.
/// `src` must have OVERSHOOT_PADDING readable slack after `src_len`.
inline size_t decodeStandalone(const uint8_t * src, size_t src_len, uint8_t * out, size_t out_capacity_bytes)
{
    BitReader reader(src, src_len, 0);
    const uint8_t * magic = reader.readAlignedBytes(MAGIC_HEADER.size());
    if (std::memcmp(magic, MAGIC_HEADER.data(), MAGIC_HEADER.size()) != 0)
        throw PcodecError("pcodec: magic header does not match");

    size_t standalone_version = reader.readU64(BITS_TO_ENCODE_STANDALONE_VERSION);
    if (standalone_version < 2 || standalone_version > CURRENT_STANDALONE_VERSION)
        throw PcodecError("pcodec: unsupported standalone version");

    std::optional<uint8_t> uniform_type;
    if (standalone_version >= 3)
    {
        uint8_t byte = reader.readAlignedBytes(1)[0];
        if (byte != MAGIC_TERMINATION_BYTE)
            uniform_type = byte;
    }
    // varint n_hint (ignored; we size the output from the column metadata)
    {
        Bitlen power = 1 + static_cast<Bitlen>(reader.readU64(BITS_TO_ENCODE_VARINT_POWER));
        reader.readU64(power);
        reader.drainEmptyByte("pcodec: standalone size hint");
    }

    uint8_t format_major = readWrappedFormatVersion(reader);

    size_t out_pos = 0;
    while (true)
    {
        uint8_t type_byte = reader.readAlignedBytes(1)[0];
        if (type_byte == MAGIC_TERMINATION_BYTE)
            break;
        if (uniform_type && *uniform_type != type_byte)
            throw PcodecError("pcodec: chunk number type does not match file uniform type");

        size_t n = reader.readU64(BITS_TO_ENCODE_N_ENTRIES) + 1;

        // Bounds-check before decoding into out.
        size_t bytes = n * widthForType(type_byte);
        if (out_pos + bytes > out_capacity_bytes)
            throw PcodecError("pcodec: decoded data exceeds expected size");
        decodeChunkByType(reader, type_byte, format_major, n, out + out_pos);
        out_pos += bytes;
    }
    return out_pos;
}

}
