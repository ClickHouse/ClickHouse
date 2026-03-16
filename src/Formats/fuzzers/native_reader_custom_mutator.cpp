/// Structure-aware custom mutator for native_reader_fuzzer.
///
/// The Native wire format has this shape (after the mode byte at offset 0):
///   [BlockInfo VarUInt flags][VarUInt column_count][VarUInt row_count]
///   [per-column: VarUInt name_len][name bytes][VarUInt type_len][type bytes]
///   [serialization_info][column data...]
///
/// This mutator preserves the mode byte and occasionally injects known-valid
/// ClickHouse type names as length-prefixed strings to help libFuzzer reach
/// the type-parsing and column-deserialization code paths.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <random>
#include <string_view>

/// Provided by libFuzzer — invokes the built-in mutation engine.
extern "C" size_t LLVMFuzzerMutate(uint8_t * Data, size_t Size, size_t MaxSize);

/// A small dictionary of valid ClickHouse type names that appear verbatim in
/// the Native wire format as VarUInt-length-prefixed UTF-8 strings.
static constexpr std::string_view kTypeNames[] = {
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Float32",
    "Float64",
    "String",
    "FixedString(16)",
    "Date",
    "Date32",
    "DateTime",
    "DateTime64(3)",
    "UUID",
    "Array(UInt64)",
    "Array(String)",
    "Tuple(UInt64, String)",
    "LowCardinality(String)",
    "LowCardinality(UInt64)",
    "Nullable(String)",
    "Nullable(UInt64)",
    "Map(String, UInt64)",
    "Dynamic",
    "Variant(UInt64, String)",
};

static constexpr size_t kTypeNamesCount = sizeof(kTypeNames) / sizeof(kTypeNames[0]);

/// Write a VarUInt-encoded value to buf, return bytes written.
static size_t writeVarUInt(uint64_t v, uint8_t * buf, size_t cap)
{
    size_t n = 0;
    do
    {
        if (n >= cap)
            return 0;
        uint8_t byte = static_cast<uint8_t>(v & 0x7F);
        v >>= 7;
        if (v != 0)
            byte |= 0x80;
        buf[n++] = byte;
    } while (v != 0);
    return n;
}

/// Serialize a length-prefixed string (VarUInt length + raw bytes) into out[0..cap).
/// Returns bytes written, or 0 if cap is insufficient.
static size_t writeLenPrefixedString(std::string_view s, uint8_t * out, size_t cap)
{
    uint8_t varint[10];
    size_t vlen = writeVarUInt(static_cast<uint64_t>(s.size()), varint, sizeof(varint));
    if (vlen == 0 || vlen + s.size() > cap)
        return 0;
    memcpy(out, varint, vlen);
    memcpy(out + vlen, s.data(), s.size());
    return vlen + s.size();
}

extern "C" size_t LLVMFuzzerCustomMutator(uint8_t * Data, size_t Size, size_t MaxSize, unsigned int Seed)
{
    /// Need at least 2 bytes: mode byte + 1 payload byte.
    if (MaxSize < 2)
        return 0;

    std::mt19937 rng(Seed);

    /// Preserve the mode byte across all mutation strategies.
    uint8_t mode_byte = (Size >= 1) ? Data[0] : static_cast<uint8_t>(rng() & 1u);

    const uint32_t strategy = rng() % 100;

    if (strategy < 15)
    {
        /// Strategy 1: flip the mode byte only, keep payload intact.
        if (Size < 1)
        {
            Data[0] = mode_byte ^ 1u;
            return 1;
        }
        Data[0] ^= 1u;
        return Size;
    }
    else if (strategy < 45)
    {
        /// Strategy 2: append a length-prefixed known type name to the payload.
        const std::string_view & tname = kTypeNames[rng() % kTypeNamesCount];
        const size_t payload_size = (Size >= 1) ? (Size - 1) : 0;
        const size_t avail = MaxSize - 1 - payload_size;

        uint8_t encoded[128];
        const size_t enc_len = writeLenPrefixedString(tname, encoded, sizeof(encoded));
        if (enc_len > 0 && enc_len <= avail)
        {
            Data[0] = mode_byte;
            memcpy(Data + 1 + payload_size, encoded, enc_len);
            return 1 + payload_size + enc_len;
        }
        /// Fall through.
    }
    else if (strategy < 60)
    {
        /// Strategy 3: overwrite a random 8-byte window in the payload with
        /// a VarUInt encoding of a boundary row/column count (0, 1, MAX-1, MAX).
        static constexpr uint64_t kBoundaries[] = {0, 1, 0x7F, 0x3FFF, 0xFFFF, 0xFFFFFFFF};
        if (Size > 1)
        {
            const size_t payload_size = Size - 1;
            const size_t offset = 1 + (rng() % payload_size);
            const uint64_t val = kBoundaries[rng() % (sizeof(kBoundaries) / sizeof(kBoundaries[0]))];
            const size_t avail = MaxSize - offset;
            size_t written = writeVarUInt(val, Data + offset, avail);
            Data[0] = mode_byte;
            if (written == 0)
                written = 0;
            return std::max(Size, offset + written);
        }
    }

    /// Default: delegate to libFuzzer's built-in mutator on the payload (Data+1).
    const size_t payload_size = (Size >= 1) ? (Size - 1) : 0;
    const size_t new_payload_size = LLVMFuzzerMutate(Data + 1, payload_size, MaxSize - 1);
    Data[0] = mode_byte;
    return 1 + new_payload_size;
}

extern "C" size_t LLVMFuzzerCustomCrossOver(
    const uint8_t * Data1,
    size_t Size1,
    const uint8_t * Data2,
    size_t Size2,
    uint8_t * Out,
    size_t MaxOutSize,
    unsigned int Seed)
{
    if (MaxOutSize < 2 || Size1 < 1 || Size2 < 1)
        return 0;

    std::mt19937 rng(Seed);

    /// Pick mode byte from one of the two parents.
    Out[0] = (rng() & 1u) ? Data1[0] : Data2[0];

    /// Interleave payload bytes from both parents in fixed-size chunks.
    const uint8_t * p1 = Data1 + 1;
    size_t s1 = Size1 - 1;
    const uint8_t * p2 = Data2 + 1;
    size_t s2 = Size2 - 1;

    size_t out_pos = 1;
    bool use_first = (rng() & 1u) != 0;

    while (out_pos < MaxOutSize)
    {
        const uint8_t * src = use_first ? p1 : p2;
        size_t src_size = use_first ? s1 : s2;

        if (src_size == 0)
        {
            use_first = !use_first;
            src = use_first ? p1 : p2;
            src_size = use_first ? s1 : s2;
            if (src_size == 0)
                break;
        }

        const size_t chunk = std::min({static_cast<size_t>(8), src_size, MaxOutSize - out_pos});
        memcpy(Out + out_pos, src, chunk);
        out_pos += chunk;

        if (use_first)
        {
            p1 += chunk;
            s1 -= chunk;
        }
        else
        {
            p2 += chunk;
            s2 -= chunk;
        }
        use_first = !use_first;
    }

    return out_pos;
}
