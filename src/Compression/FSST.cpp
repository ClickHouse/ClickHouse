#include <Compression/FSST.h>

#include <Common/Exception.h>
#include <base/defines.h>
#include <base/unaligned.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>


namespace DB
{

namespace FSST
{

namespace
{

UInt64 symbolMask(size_t length)
{
    return length >= 8 ? ~UInt64(0) : (UInt64(1) << (8 * length)) - 1;
}

/// Loads up to 8 bytes as a little-endian value, the missing bytes are zero.
UInt64 loadWord(const char * pos, size_t remaining)
{
    if (remaining >= 8)
        return unalignedLoadLittleEndian<UInt64>(pos);

    UInt64 word = 0;
    for (size_t i = 0; i < remaining; ++i)
        word |= UInt64(static_cast<UInt8>(pos[i])) << (8 * i);
    return word;
}

void storeSymbol(char * dest, UInt64 symbol, size_t length)
{
    for (size_t i = 0; i < length; ++i)
        dest[i] = static_cast<char>(symbol >> (8 * i));
}

/// The symbols of at least 3 bytes are found by a hash of their first 3 bytes.
/// Only one such symbol is allowed in a slot, so the table builder skips a symbol whose slot is taken.
constexpr size_t HASH_TABLE_SIZE_LOG2 = 11;
constexpr size_t HASH_TABLE_SIZE = 1 << HASH_TABLE_SIZE_LOG2;

size_t hashPrefix(UInt64 word)
{
    return ((word & 0xFFFFFF) * 0x9E3779B97F4A7C15ULL) >> (64 - HASH_TABLE_SIZE_LOG2);
}

/// Finds the longest symbol at a position of the data.
/// A match is `(length << 8) | code`, where the code is `ESCAPE_CODE` and the length is 1 if there is no symbol.
class Encoder
{
public:
    explicit Encoder(const SymbolTable & table)
    {
        for (size_t i = 0; i < 256; ++i)
            byte_codes[i] = (1 << 8) | ESCAPE_CODE;

        for (size_t code = 0; code < table.num_symbols; ++code)
            if (table.lengths[code] == 1)
                byte_codes[table.symbols[code] & 0xFF] = static_cast<UInt16>((1 << 8) | code);

        for (size_t i = 0; i < 65536; ++i)
            short_codes[i] = byte_codes[i & 0xFF];

        for (size_t code = 0; code < table.num_symbols; ++code)
        {
            UInt64 symbol = table.symbols[code];
            size_t length = table.lengths[code];
            if (length == 2)
                short_codes[symbol & 0xFFFF] = static_cast<UInt16>((2 << 8) | code);
            else if (length >= 3)
                slots[hashPrefix(symbol)] = {symbol, symbolMask(length), static_cast<UInt16>((length << 8) | code)};
        }
    }

    /// For a position with at least 8 more bytes.
    ALWAYS_INLINE UInt16 matchFast(const char * pos) const
    {
        const UInt64 word = unalignedLoadLittleEndian<UInt64>(pos);
        const Slot & slot = slots[hashPrefix(word)];
        if (slot.match && ((word ^ slot.symbol) & slot.mask) == 0)
            return slot.match;
        return short_codes[word & 0xFFFF];
    }

    ALWAYS_INLINE UInt16 match(const char * pos, size_t remaining) const
    {
        if (remaining >= 8)
            return matchFast(pos);

        const UInt64 word = loadWord(pos, remaining);
        const Slot & slot = slots[hashPrefix(word)];
        if (slot.match && (slot.match >> 8) <= remaining && ((word ^ slot.symbol) & slot.mask) == 0)
            return slot.match;
        return remaining >= 2 ? short_codes[word & 0xFFFF] : byte_codes[word & 0xFF];
    }

private:
    struct Slot
    {
        UInt64 symbol = 0;
        UInt64 mask = 0;
        UInt16 match = 0;
    };

    /// The best match of at most 2 bytes for the first two bytes at a position.
    std::array<UInt16, 65536> short_codes;
    /// The same for the last byte of the data.
    std::array<UInt16, 256> byte_codes;
    std::array<Slot, HASH_TABLE_SIZE> slots{};
};

/// Up to this many bytes of the data are used to build the symbol table.
constexpr size_t SAMPLE_SIZE = 16 * 1024;
constexpr size_t SAMPLE_CHUNK_SIZE = 512;
/// The number of generations of the table: each of them can at most double the length of the symbols.
constexpr size_t NUM_GENERATIONS = 5;

/// A symbol or an escaped byte in the counters: codes below 256 are symbols, the others are bytes.
constexpr size_t NUM_PSEUDO_CODES = 512;

struct Candidate
{
    UInt64 symbol = 0;
    UInt64 length = 0;
    UInt64 gain = 0;
};

/// Sums the gains of the candidates with the same symbol. The number of candidates is bounded
/// by the number of symbols and pairs of adjacent symbols in the sample.
class CandidateMap
{
public:
    CandidateMap() : cells(CAPACITY) {}

    void add(UInt64 symbol, size_t length, UInt64 gain)
    {
        const UInt64 key = symbol ^ (UInt64(length) << 56);
        size_t i = (key * 0x9E3779B97F4A7C15ULL) >> (64 - CAPACITY_LOG2);
        while (true)
        {
            Candidate & cell = cells[i];
            if (cell.length == 0)
            {
                cell = {symbol, length, gain};
                used.push_back(static_cast<UInt32>(i));
                return;
            }
            if (cell.symbol == symbol && cell.length == length)
            {
                cell.gain += gain;
                return;
            }
            i = (i + 1) & (CAPACITY - 1);
        }
    }

    /// Moves the candidates to `result` and clears the map.
    void extract(VectorWithMemoryTracking<Candidate> & result)
    {
        result.clear();
        for (UInt32 i : used)
        {
            result.push_back(cells[i]);
            cells[i] = {};
        }
        used.clear();
    }

private:
    /// More than twice the maximum number of candidates: the sample has at most `SAMPLE_SIZE` symbols,
    /// each of them is a candidate along with its pair with the previous symbol, and its first byte.
    static constexpr size_t CAPACITY_LOG2 = 16;
    static constexpr size_t CAPACITY = 1 << CAPACITY_LOG2;
    static_assert(CAPACITY > 3 * SAMPLE_SIZE);

    /// The scratch memory of a thread, reused by all queries, so it is not accounted to any of them.
    std::vector<Candidate> cells; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<UInt32> used; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
};

/// The counters of the symbols and the pairs of adjacent symbols in the sample.
struct Counters
{
    /// The scratch memory of a thread, reused by all queries, so it is not accounted to any of them.
    std::vector<UInt16> count1 = std::vector<UInt16>(NUM_PSEUDO_CODES); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<UInt16> count2 = std::vector<UInt16>(NUM_PSEUDO_CODES * NUM_PSEUDO_CODES); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<UInt32> touched_pairs; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    static_assert(SAMPLE_SIZE < 65536, "the counters of the sample must fit 16 bits");

    void reset()
    {
        std::fill(count1.begin(), count1.end(), 0);
        for (UInt32 pair : touched_pairs)
            count2[pair] = 0;
        touched_pairs.clear();
    }
};

/// The order of candidates: by gain, then the longer symbol, then the symbol bytes, so that the table is deterministic.
bool betterCandidate(const Candidate & a, const Candidate & b)
{
    if (a.gain != b.gain)
        return a.gain > b.gain;
    if (a.length != b.length)
        return a.length > b.length;
    return a.symbol < b.symbol;
}

}


SymbolTable SymbolTable::build(const char * data, size_t size, const std::span<const UInt32> * value_boundaries)
{
    /// Chunks of the sample: the whole data if it is small, or evenly spaced pieces of it.
    VectorWithMemoryTracking<std::pair<const char *, size_t>> sample;
    if (size <= SAMPLE_SIZE)
    {
        sample.emplace_back(data, size);
    }
    else
    {
        const size_t num_chunks = SAMPLE_SIZE / SAMPLE_CHUNK_SIZE;
        for (size_t i = 0; i < num_chunks; ++i)
            sample.emplace_back(data + (size - SAMPLE_CHUNK_SIZE) / (num_chunks - 1) * i, SAMPLE_CHUNK_SIZE);
    }

    /// The memory of the counters and the candidates is large enough to make allocating and zeroing it on every call slow.
    thread_local Counters counters;
    thread_local CandidateMap candidate_map;
    VectorWithMemoryTracking<Candidate> candidates;
    std::array<bool, HASH_TABLE_SIZE> used_slots{};

    SymbolTable table;

    for (size_t generation = 0; generation < NUM_GENERATIONS; ++generation)
    {
        auto encoder = std::make_unique<Encoder>(table);
        counters.reset();
        auto & count1 = counters.count1;
        auto & count2 = counters.count2;

        /// Encode the sample with the current table and count the symbols and the pairs of adjacent symbols.
        /// With the boundaries of the values, a chunk is encoded value by value, as `encode` does,
        /// and the pairs of symbols of different values are not counted.
        for (const auto & [chunk, chunk_size] : sample)
        {
            const UInt32 chunk_offset = static_cast<UInt32>(chunk - data);
            const UInt32 * next_boundary = nullptr;
            const UInt32 * boundaries_end = nullptr;
            if (value_boundaries)
            {
                next_boundary = std::upper_bound(value_boundaries->data(), value_boundaries->data() + value_boundaries->size(), chunk_offset);
                boundaries_end = value_boundaries->data() + value_boundaries->size();
            }
            auto next_segment_end = [&]() -> size_t
            {
                if (next_boundary != boundaries_end && *next_boundary < chunk_offset + chunk_size)
                    return *next_boundary++ - chunk_offset;
                return chunk_size;
            };
            size_t segment_end = next_segment_end();

            size_t prev = NUM_PSEUDO_CODES;
            size_t pos = 0;
            while (pos < chunk_size)
            {
                if (pos == segment_end)
                {
                    segment_end = next_segment_end();
                    prev = NUM_PSEUDO_CODES;
                }

                const UInt16 match = encoder->match(chunk + pos, segment_end - pos);
                const UInt8 code = static_cast<UInt8>(match & 0xFF);
                const size_t length = match >> 8;
                const size_t first_byte_pseudo_code = 256 + static_cast<UInt8>(chunk[pos]);
                const size_t pseudo_code = code == ESCAPE_CODE ? first_byte_pseudo_code : code;

                ++count1[pseudo_code];
                /// Also count the first byte alone: it is a candidate in case the longer symbol is not kept.
                if (length > 1)
                    ++count1[first_byte_pseudo_code];

                if (prev != NUM_PSEUDO_CODES)
                {
                    UInt32 pair = static_cast<UInt32>(prev * NUM_PSEUDO_CODES + pseudo_code);
                    if (count2[pair]++ == 0)
                        counters.touched_pairs.push_back(pair);
                }

                prev = pseudo_code;
                pos += length;
            }
        }

        auto symbol_of = [&](size_t pseudo_code) -> std::pair<UInt64, size_t>
        {
            if (pseudo_code >= 256)
                return {pseudo_code - 256, 1};
            return {table.symbols[pseudo_code], table.lengths[pseudo_code]};
        };

        /// The gain of a symbol is the number of bytes it would cover in the sample.
        for (size_t pseudo_code = 0; pseudo_code < NUM_PSEUDO_CODES; ++pseudo_code)
        {
            if (!count1[pseudo_code])
                continue;
            auto [symbol, length] = symbol_of(pseudo_code);
            UInt64 gain = UInt64(count1[pseudo_code]) * length;
            /// Promote single bytes (as the reference implementation does): they reduce the number of escapes,
            /// which take two bytes each.
            if (length == 1)
                gain *= 8;
            candidate_map.add(symbol, length, gain);
        }

        /// The concatenations of adjacent symbols are the candidates for longer symbols.
        for (UInt32 pair : counters.touched_pairs)
        {
            auto [symbol1, length1] = symbol_of(pair / NUM_PSEUDO_CODES);
            if (length1 >= MAX_SYMBOL_LENGTH)
                continue;
            auto [symbol2, length2] = symbol_of(pair % NUM_PSEUDO_CODES);
            size_t length = std::min(MAX_SYMBOL_LENGTH, length1 + length2);
            UInt64 symbol = (symbol1 | (symbol2 << (8 * length1))) & symbolMask(length);
            candidate_map.add(symbol, length, UInt64(count2[pair]) * length);
        }

        candidate_map.extract(candidates);

        /// Only the best candidates can get into the table. Twice as many as fit, because some of the long ones
        /// are skipped when their slot is taken.
        const size_t num_best = std::min(candidates.size(), 2 * MAX_SYMBOLS);
        std::nth_element(candidates.begin(), candidates.begin() + num_best, candidates.end(), betterCandidate);
        std::sort(candidates.begin(), candidates.begin() + num_best, betterCandidate);

        SymbolTable new_table;
        used_slots.fill(false);
        for (size_t i = 0; i < num_best && new_table.num_symbols < MAX_SYMBOLS; ++i)
        {
            const auto & candidate = candidates[i];
            if (candidate.length >= 3)
            {
                const size_t slot = hashPrefix(candidate.symbol);
                if (used_slots[slot])
                    continue;
                used_slots[slot] = true;
            }

            new_table.symbols[new_table.num_symbols] = candidate.symbol;
            new_table.lengths[new_table.num_symbols] = static_cast<UInt8>(candidate.length);
            ++new_table.num_symbols;
        }

        table = new_table;
    }

    return table;
}

size_t SymbolTable::serializedSize() const
{
    size_t res = 1 + num_symbols;
    for (size_t code = 0; code < num_symbols; ++code)
        res += lengths[code];
    return res;
}

void SymbolTable::serialize(char * dest) const
{
    *dest++ = static_cast<char>(num_symbols);
    for (size_t code = 0; code < num_symbols; ++code)
        *dest++ = static_cast<char>(lengths[code]);
    for (size_t code = 0; code < num_symbols; ++code)
    {
        storeSymbol(dest, symbols[code], lengths[code]);
        dest += lengths[code];
    }
}

size_t SymbolTable::deserialize(const char * src, size_t size, int error_code)
{
    *this = {};

    if (size < 1)
        throw Exception(error_code, "Cannot read the FSST symbol table: the data is too short");

    num_symbols = static_cast<UInt8>(src[0]);
    if (num_symbols > MAX_SYMBOLS)
        throw Exception(error_code, "Cannot read the FSST symbol table: too many symbols ({})", num_symbols);
    if (size < 1 + num_symbols)
        throw Exception(error_code, "Cannot read the FSST symbol table: the data is too short");

    size_t pos = 1 + num_symbols;
    for (size_t code = 0; code < num_symbols; ++code)
    {
        size_t length = static_cast<UInt8>(src[1 + code]);
        if (length < 1 || length > MAX_SYMBOL_LENGTH)
            throw Exception(error_code, "Cannot read the FSST symbol table: wrong length of a symbol ({})", length);
        if (size - pos < length)
            throw Exception(error_code, "Cannot read the FSST symbol table: the data is too short");

        lengths[code] = static_cast<UInt8>(length);
        symbols[code] = loadWord(src + pos, length);
        pos += length;
    }

    return pos;
}


namespace
{

/// The escaped byte is written after every code, without a branch: `dest` has room for two bytes per byte of the data.
ALWAYS_INLINE void writeMatch(UInt16 match, const char *& pos, UInt8 *& out)
{
    const UInt8 code = static_cast<UInt8>(match & 0xFF);
    out[0] = code;
    out[1] = static_cast<UInt8>(*pos);
    out += 1 + (code == ESCAPE_CODE);
    pos += match >> 8;
}

}

size_t encode(const SymbolTable & table, const char * src, size_t size, UInt8 * dest, const std::span<const UInt32> * value_boundaries)
{
    auto encoder = std::make_unique<Encoder>(table);

    /// The position of the next symbol depends on the length of the previous one, so the latency of the lookups
    /// limits the speed. To hide it, the data is split into lanes, which are encoded together, interleaved,
    /// and their codes are concatenated. A symbol does not cross the boundary of two lanes, and the decoder
    /// does not need to know about them. With the boundaries of the values, the lanes are split at them,
    /// and every lane is encoded value by value (a segment is a value, or a part of it at the edge of the data).
    struct Lane
    {
        const char * pos;
        const char * end;
        const char * segment_end;
        const UInt32 * next_boundary;
        const UInt32 * boundaries_end;
        /// Every lane writes into the part of `dest` for two bytes per byte of its data.
        UInt8 * out_begin;
        UInt8 * out;
    };

    static constexpr size_t NUM_LANES = 4;
    static constexpr size_t MIN_LANE_SIZE = 1024;
    const size_t num_lanes = size >= NUM_LANES * MIN_LANE_SIZE ? NUM_LANES : 1;

    const UInt32 * boundaries_begin = value_boundaries ? value_boundaries->data() : nullptr;
    const UInt32 * boundaries_end = value_boundaries ? value_boundaries->data() + value_boundaries->size() : nullptr;

    std::array<size_t, NUM_LANES + 1> splits{};
    splits[num_lanes] = size;
    for (size_t i = 1; i < num_lanes; ++i)
    {
        size_t split = size * i / num_lanes;
        if (value_boundaries)
        {
            const UInt32 * it = std::lower_bound(boundaries_begin, boundaries_end, split);
            split = it == boundaries_end ? size : *it;
        }
        splits[i] = std::max(split, splits[i - 1]);
    }

    std::array<Lane, NUM_LANES> lanes;
    for (size_t i = 0; i < num_lanes; ++i)
    {
        Lane & lane = lanes[i];
        lane.pos = src + splits[i];
        lane.end = src + splits[i + 1];
        lane.next_boundary = value_boundaries ? std::upper_bound(boundaries_begin, boundaries_end, splits[i]) : nullptr;
        lane.boundaries_end = value_boundaries ? std::lower_bound(boundaries_begin, boundaries_end, splits[i + 1]) : nullptr;
        lane.segment_end = lane.next_boundary != lane.boundaries_end ? src + *lane.next_boundary++ : lane.end;
        lane.out_begin = dest + 2 * splits[i];
        lane.out = lane.out_begin;
    }

    auto step = [&](Lane & lane)
    {
        const size_t remaining = lane.segment_end - lane.pos;
        writeMatch(remaining >= 8 ? encoder->matchFast(lane.pos) : encoder->match(lane.pos, remaining), lane.pos, lane.out);
        if (lane.pos == lane.segment_end)
            lane.segment_end = lane.next_boundary != lane.boundaries_end ? src + *lane.next_boundary++ : lane.end;
    };

    if (num_lanes == NUM_LANES)
    {
        auto all_lanes_have_data = [&]
        {
            bool res = true;
            for (const auto & lane : lanes)
                res &= lane.pos < lane.end;
            return res;
        };

        while (all_lanes_have_data())
            for (auto & lane : lanes)
                step(lane);
    }

    UInt8 * out = dest;
    for (size_t i = 0; i < num_lanes; ++i)
    {
        Lane & lane = lanes[i];
        while (lane.pos < lane.end)
            step(lane);

        /// The codes of the previous lanes take at most two bytes per byte, so this does not overwrite the codes of the next lanes.
        const size_t lane_size = lane.out - lane.out_begin;
        if (out != lane.out_begin)
            memmove(out, lane.out_begin, lane_size);
        out += lane_size;
    }

    return out - dest;
}

size_t encodeValue(const SymbolTable & table, const char * src, size_t size, UInt8 * dest)
{
    /// The encoder takes the longest symbol at every position: at most one symbol of at least 3 bytes can match
    /// (one per slot, see `hashPrefix`), and otherwise the longest of the shorter ones. So it is the same
    /// as the longest matching symbol, which is found among the symbols with the same first byte.
    std::array<UInt8, 257> bucket_begins{};
    std::array<UInt8, MAX_SYMBOLS> codes_by_first_byte{};
    for (size_t code = 0; code < table.num_symbols; ++code)
        ++bucket_begins[(table.symbols[code] & 0xFF) + 1];
    for (size_t byte = 0; byte < 256; ++byte)
        bucket_begins[byte + 1] = static_cast<UInt8>(bucket_begins[byte + 1] + bucket_begins[byte]);
    std::array<UInt8, 256> next_in_bucket = {};
    for (size_t byte = 0; byte < 256; ++byte)
        next_in_bucket[byte] = bucket_begins[byte];
    for (size_t code = 0; code < table.num_symbols; ++code)
        codes_by_first_byte[next_in_bucket[table.symbols[code] & 0xFF]++] = static_cast<UInt8>(code);

    UInt8 * out = dest;
    size_t pos = 0;
    while (pos < size)
    {
        const size_t remaining = size - pos;
        const UInt64 word = loadWord(src + pos, remaining);
        const size_t first_byte = word & 0xFF;

        size_t best_length = 0;
        UInt8 best_code = ESCAPE_CODE;
        for (size_t i = bucket_begins[first_byte]; i < bucket_begins[first_byte + 1]; ++i)
        {
            const UInt8 code = codes_by_first_byte[i];
            const size_t length = table.lengths[code];
            if (length > best_length && length <= remaining && ((word ^ table.symbols[code]) & symbolMask(length)) == 0)
            {
                best_length = length;
                best_code = code;
            }
        }

        *out++ = best_code;
        if (best_code == ESCAPE_CODE)
        {
            *out++ = static_cast<UInt8>(first_byte);
            best_length = 1;
        }
        pos += best_length;
    }
    return out - dest;
}

void findBoundaryCodePositions(
    const SymbolTable & table, const UInt8 * codes, size_t codes_size, std::span<const UInt32> value_boundaries, VectorWithMemoryTracking<UInt32> & result)
{
    size_t pos = 0;
    size_t decoded = 0;
    for (UInt32 boundary : value_boundaries)
    {
        while (decoded < boundary && pos < codes_size)
        {
            const UInt8 code = codes[pos];
            decoded += code == ESCAPE_CODE ? 1 : table.lengths[code];
            pos += code == ESCAPE_CODE ? 2 : 1;
        }
        /// The encoding is aligned with the values.
        chassert(decoded == boundary);
        result.push_back(static_cast<UInt32>(pos));
    }
}

namespace
{

[[noreturn]] void throwMalformedCodes(int error_code)
{
    throw Exception(error_code, "Cannot decode FSST data: the codes are malformed or end before the data");
}

}

void decode(const SymbolTable & table, const UInt8 * codes, size_t codes_size, Cursor & cursor, char * dest, size_t size, int error_code)
{
    char * out = dest;
    char * const out_end = dest + size;
    size_t pos = cursor.code_pos;

    /// The rest of a symbol that was partially consumed.
    if (cursor.offset_in_symbol && out < out_end)
    {
        if (pos >= codes_size)
            throwMalformedCodes(error_code);
        const UInt8 code = codes[pos];
        const size_t length = table.lengths[code];
        if (cursor.offset_in_symbol >= length)
            throwMalformedCodes(error_code);
        const size_t n = std::min<size_t>(length - cursor.offset_in_symbol, out_end - out);
        storeSymbol(out, table.symbols[code] >> (8 * cursor.offset_in_symbol), n);
        out += n;
        cursor.offset_in_symbol += n;
        if (cursor.offset_in_symbol == length)
        {
            cursor.offset_in_symbol = 0;
            ++pos;
        }
    }

    /// The fast path writes 8 bytes for every symbol. An unused code has the length 0 in the table, so it does not
    /// advance the output, and it is detected after the loop, without a branch in it.
    /// Every iteration consumes at most 2 codes and writes at most 8 bytes, so batches of iterations
    /// that cannot reach the end of the codes or the output need no checks inside.
    bool has_unused_codes = false;
    while (true)
    {
        size_t iterations = std::min<size_t>((out_end - out) / 8, (codes_size - pos) / 2);
        if (!iterations)
            break;

        for (; iterations; --iterations)
        {
            const UInt8 code = codes[pos];
            if (unlikely(code == ESCAPE_CODE))
            {
                *out++ = static_cast<char>(codes[pos + 1]);
                pos += 2;
            }
            else
            {
                const size_t length = table.lengths[code];
                has_unused_codes |= length == 0;
                unalignedStoreLittleEndian<UInt64>(out, table.symbols[code]);
                out += length;
                ++pos;
            }
        }
    }
    if (has_unused_codes)
        throwMalformedCodes(error_code);

    while (out < out_end)
    {
        if (pos >= codes_size)
            throwMalformedCodes(error_code);

        const UInt8 code = codes[pos];
        if (code == ESCAPE_CODE)
        {
            if (pos + 1 >= codes_size)
                throwMalformedCodes(error_code);
            *out++ = static_cast<char>(codes[pos + 1]);
            pos += 2;
            continue;
        }

        const size_t length = table.lengths[code];
        if (length == 0)
            throwMalformedCodes(error_code);
        const size_t n = std::min<size_t>(length, out_end - out);
        storeSymbol(out, table.symbols[code], n);
        out += n;
        if (n < length)
        {
            cursor.offset_in_symbol = n;
            break;
        }
        ++pos;
    }

    cursor.code_pos = pos;
}

void decodeBlock(const SymbolTable & table, const UInt8 * codes, size_t codes_size, char * dest, size_t size, int error_code)
{
    Cursor cursor;
    decode(table, codes, codes_size, cursor, dest, size, error_code);
    if (cursor.code_pos != codes_size || cursor.offset_in_symbol != 0)
        throw Exception(error_code, "Cannot decode FSST data: the codes do not end with the data");
}

void skip(const SymbolTable & table, const UInt8 * codes, size_t codes_size, Cursor & cursor, size_t size, int error_code)
{
    size_t pos = cursor.code_pos;
    size_t remaining = size;

    if (cursor.offset_in_symbol && remaining)
    {
        if (pos >= codes_size)
            throwMalformedCodes(error_code);
        const size_t length = table.lengths[codes[pos]];
        if (cursor.offset_in_symbol >= length)
            throwMalformedCodes(error_code);
        const size_t n = std::min(length - cursor.offset_in_symbol, remaining);
        remaining -= n;
        cursor.offset_in_symbol += n;
        if (cursor.offset_in_symbol == length)
        {
            cursor.offset_in_symbol = 0;
            ++pos;
        }
    }

    while (remaining)
    {
        if (unlikely(pos >= codes_size))
            throwMalformedCodes(error_code);

        const UInt8 code = codes[pos];
        if (unlikely(code == ESCAPE_CODE))
        {
            --remaining;
            pos += 2;
            continue;
        }

        const size_t length = table.lengths[code];
        if (unlikely(length == 0))
            throwMalformedCodes(error_code);
        if (length > remaining)
        {
            cursor.offset_in_symbol = remaining;
            break;
        }
        remaining -= length;
        ++pos;
    }

    /// An escape code at the end of the codes.
    if (pos > codes_size)
        throwMalformedCodes(error_code);
    cursor.code_pos = pos;
}

}

}
