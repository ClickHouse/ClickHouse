#include <Compression/Wallaby.h>

#include <Common/Exception.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <memory>
#include <numeric>
#include <vector>

/** Wallaby: lossless compression of `Float32` and `Float64` columns.
  *
  * The codec turns the floating-point values of a block into integers that are cheap to describe, predicts every
  * integer from the previous ones, and codes the residuals with an entropy coder whose alphabet is fitted to the
  * block. Three ideas make it compress better than a bit-packing codec (`ALP`) and than a codec with a fixed
  * family of bins (`PCO`) on blocks of a few thousand values:
  *   - the integers come from the best of several front-ends, chosen per chunk by measuring: bit patterns with the
  *     common trailing zeros removed, a division by a denominator (decimals, but also averages over 3, 12 or 60
  *     samples: any rational grid), or a multiplication (values that were produced by one);
  *   - the alphabet of the entropy coder mixes dictionary symbols for frequent exact values with bins for the rest,
  *     and zero runs are symbols of their own;
  *   - the header of a chunk is small: the bins are described by a few parameters instead of explicit bounds.
  *
  * ## Layout
  *
  * All multi-byte integers and all bit streams are little-endian; bit fields are filled from the least
  * significant bit.
  *
  * Block:
  *   byte 0: bits 0-3: format version (2); bit 4: 1 for `Float64`, 0 for `Float32`;
  *           bit 5: the block is stored verbatim (the uncompressed data follows); bits 6-7: zero.
  *   chunks: each covers min(16384, remaining) values;
  *   tail:   (uncompressed size) mod (float width) bytes verbatim.
  *
  * Chunk: the two lowest bits of the first byte are the mode.
  *   0 RAW:     the values verbatim.
  *   1 CONST:   one value; all the values of the chunk are equal to it bitwise.
  *   2 ENTROPY: a header in a bit stream, then byte-aligned payloads.
  *
  * Header of an ENTROPY chunk (after the 2 bits of the mode):
  *   front-end, 2 bits:
  *     0 BITS: shift (6 bits). latent = orderedBits(value) >> shift, where orderedBits maps the bit pattern to an
  *             unsigned integer preserving the order of the floats, and shift is the number of trailing zero bits
  *             common to all the values.
  *     1 DIV:  value = Float(latent / scale).
  *     2 MUL:  value = Float(latent * scale).
  *     For DIV and MUL: single (1 bit; `Float32` only: the operation is done in single precision), scale kind (2 bits):
  *     0: scale as a `Float64` (64 bits); 1: a power of ten, 10^e for DIV and 1 / 10^e for MUL (e: 5 bits);
  *     2 (MUL only): 1 / 10^e rounded to `Float32`.
  *   multiplier flag (1 bit) and the multiplier g (signed number, see below), g >= 2;
  *   base (signed number):  latent = base + g * m;
  *   order (3 bits): m is differenced `order` times; if order > 0: center (signed number) and `order` initial
  *     values (signed numbers): the difference of order j at the position j;
  *   adjustments flag (1 bit): a stream of adjustments follows the main stream;
  *   exceptions kind (2 bits): 0 none, 1 verbatim, 2 coded; if not 0: number of exceptions (15 bits);
  *     if coded: base of the exception values (signed number);
  *   the model and the sizes of the main stream; of the stream of adjustments; of the two streams of exceptions
  *     (gaps between positions, values).
  * A signed number: the length of the magnitude in bits (7 bits); if not zero: the sign (1 bit) and the magnitude.
  *
  * After the header, byte-aligned: verbatim exceptions (positions as `UInt16`, then the values), the payload of the
  * main stream, of the adjustments, of the gaps and of the values of the coded exceptions.
  *
  * Exceptions are values that have no integer (NaN, infinities, out of range) or, when only a few values are not
  * reproduced exactly, those values. They are removed from the sequence: the streams code the other (regular)
  * values in order. An adjustment is the distance from the reconstruction to the value in the order of the bit
  * patterns: value = fromOrdered(orderedBits(reconstruction) + adjustment).
  *
  * ## Streams
  *
  * A stream codes a sequence of signed integers (residuals; zero is the most likely value). Every value is a token
  * plus raw bits: value = lower(token) + raw(nbits(token)) + center. The alphabet of tokens, in this order:
  *   - dictionary: exact values (no raw bits);
  *   - bins of one of two families:
  *       classes: the sign, and for magnitudes below 2^split the magnitude itself, above: the exponent and the
  *         `msb` bits below the leading one; the remaining bits are raw (at most 56: larger exponents keep more
  *         bits in the token);
  *       fixed:   bin j covers [min + (j << shift), min + ((j + 1) << shift));
  *   - zero runs: the token of exponent e stands for 2^e + raw(e) zeros in a row.
  *
  * Model of a stream:
  *   family (1 bit); fixed: shift (6 bits), min (signed number); classes: split (3 bits), msb (3 bits);
  *   dictionary: number of entries (13 bits); if not zero: Exp-Golomb order (6 bits), the first value (signed
  *     number), then the gaps between the sorted values minus one, Exp-Golomb coded;
  *   bins: number of used bins (13 bits); if not zero: Exp-Golomb order (4 bits), the identifiers of the bins in
  *     increasing order: the first, then the gaps minus one. The identifier of a class is (class << 1) | sign;
  *   runs flag (1 bit); if set: the mask of the used exponents (15 bits);
  *   contexts: number minus one (2 bits), thresholds (7 bits each): the context of a token is the number of
  *     thresholds below the bit length of the magnitude of the token 4 positions back (same ANS state), 0 at the
  *     start. Every context has its own frequencies;
  *   table size log2 minus 5 (3 bits);
  *   for every context, the frequencies of all the tokens but the last (it is implied by the sum, 2^log):
  *     mode (4 bits): 0..12: Exp-Golomb codes of this order; 15: for each, the bit length (4 bits) and the bits below
  *     the leading one. Nothing if the alphabet has a single token;
  *   number of tokens minus one (14 bits), size of the token stream in bytes (16 bits), of the raw bits (18 bits).
  *
  * Payload of a stream: the token stream (tANS with 4 interleaved states: the initial states, `log` bits each, then
  * the bits of the tokens in order), then the raw bits of the tokens in order.
  *
  * ## Safety
  *
  * The decoder validates every length, count and index of the header and never reads outside of the compressed
  * data or writes outside of the output; a malformed block gives an exception. The states of the entropy decoder
  * stay inside of the tables for any input by construction, and their final values are checked.
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int LOGICAL_ERROR;
}

namespace Wallaby
{

namespace
{


/// A copy of raw bytes; an empty range may come with a null pointer.
void copyBytes(void * to, const void * from, size_t size)
{
    if (size)
        std::memcpy(to, from, size);
}

[[noreturn]] void corrupt(const char * message)
{
    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data: {}", message);
}

[[noreturn]] void logicalError(const char * message)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wallaby codec: {}", message);
}

/// The number of values coded with one model. A compress block is split into chunks of at most this size.
constexpr UInt32 CHUNK_MAX = 16384;
constexpr UInt32 MAX_ALPHABET = 4096;
constexpr UInt32 MAX_TABLE_LOG = 12;
constexpr UInt32 MIN_TABLE_LOG = 5;
/// Raw (uncoded) bits of one token; a 64-bit unaligned load serves any bit offset.
constexpr UInt32 MAX_RAW_BITS = 56;
constexpr UInt32 ANS_LANES = 4;
constexpr UInt32 MAX_RUN_EXP = 14;
static_assert((1u << MAX_RUN_EXP) == CHUNK_MAX);

UInt32 bitLength(UInt64 x) { return x ? 64 - std::countl_zero(x) : 0; }
UInt64 lowMask(UInt32 bits) { return bits >= 64 ? ~UInt64(0) : (UInt64(1) << bits) - 1; }
UInt64 magnitude(Int64 x) { return x < 0 ? UInt64(0) - UInt64(x) : UInt64(x); }

UInt64 loadLE64(const UInt8 * p)
{
    return unalignedLoadLittleEndian<UInt64>(p);
}

void storeLE64(UInt8 * p, UInt64 v)
{
    unalignedStoreLittleEndian<UInt64>(p, v);
}

/// A buffer of trivially constructible elements that is not filled with zeros when it is allocated or grown.
template <typename T>
struct Scratch
{
    std::unique_ptr<T[]> holder;
    size_t capacity = 0;

    T * resize(size_t size)
    {
        if (size > capacity)
        {
            holder = std::make_unique_for_overwrite<T[]>(size);
            capacity = size;
        }
        return holder.get();
    }

    T * data() { return holder.get(); }
    const T * data() const { return holder.get(); }
};

/// LSB-first bit writer. Needs 8 writable bytes after the current position.
struct BitWriter
{
    UInt8 * p;
    UInt64 acc = 0;
    UInt32 cnt = 0;

    explicit BitWriter(UInt8 * out) : p(out) {}

    /// nbits <= 56, value < 2^nbits
    inline void put(UInt64 value, UInt32 nbits)
    {
        acc |= value << cnt;
        cnt += nbits;
        storeLE64(p, acc);
        p += cnt >> 3;
        acc >>= (cnt & ~7u);
        cnt &= 7;
    }

    inline void put64(UInt64 value, UInt32 nbits)
    {
        if (nbits > 32)
        {
            put(value & 0xffffffffu, 32);
            put(value >> 32, nbits - 32);
        }
        else
            put(value & lowMask(nbits), nbits);
    }

    /// Exp-Golomb code of order k: handles both small and occasionally large numbers.
    void putExpGolomb(UInt64 value, UInt32 k)
    {
        UInt64 x = (value >> k) + 1;
        UInt32 len = bitLength(x);          /// >= 1
        put64(0, len - 1);               /// len - 1 zeros
        put(1, 1);
        put64(x & lowMask(len - 1), len - 1);
        put64(value & lowMask(k), k);
    }

    /// A signed number: 7 bits of magnitude length, the sign, the magnitude.
    void putSigned(Int64 value)
    {
        UInt64 mag = magnitude(value);
        UInt32 len = bitLength(mag);
        put(len, 7);
        if (len)
        {
            put(value < 0, 1);
            put64(mag, len);
        }
    }

    UInt8 * finish()
    {
        if (cnt)
        {
            *p++ = UInt8(acc);
            acc = 0;
            cnt = 0;
        }
        return p;
    }
};

/// LSB-first bit reader with bounds checks, for headers. The buffer must have 8 readable bytes after `end`.
struct BitReader
{
    const UInt8 * base;
    UInt64 pos = 0;
    UInt64 limit;

    BitReader(const UInt8 * data, size_t size) : base(data), limit(UInt64(size) * 8) {}

    inline UInt64 get(UInt32 nbits)   /// nbits <= 56
    {
        if (pos + nbits > limit)
            corrupt("truncated header");
        UInt64 w = loadLE64(base + (pos >> 3)) >> (pos & 7);
        pos += nbits;
        return w & lowMask(nbits);
    }

    inline UInt64 get64(UInt32 nbits)
    {
        if (nbits > 32)
        {
            UInt64 lo = get(32);
            return lo | (get(nbits - 32) << 32);
        }
        return get(nbits);
    }

    UInt64 getExpGolomb(UInt32 k)
    {
        /// Fast path: the whole code is within one 64-bit load.
        if (pos + 64 <= limit)
        {
            UInt64 w = loadLE64(base + (pos >> 3)) >> (pos & 7);
            UInt32 zeros = UInt32(std::countr_zero(w | (UInt64(1) << 63)));
            if (2 * zeros + 1 + k <= 56)
            {
                UInt64 x = (UInt64(1) << zeros) | ((w >> (zeros + 1)) & lowMask(zeros));
                UInt64 low = (w >> (2 * zeros + 1)) & lowMask(k);
                pos += 2 * zeros + 1 + k;
                return ((x - 1) << k) | low;
            }
        }
        UInt32 zeros = 0;
        while (get(1) == 0)
        {
            if (++zeros >= 64)
                corrupt("bad Exp-Golomb code");
        }
        UInt64 x = (UInt64(1) << zeros) | get64(zeros);
        UInt64 hi = x - 1;
        if (k && (hi >> (64 - k)))
            corrupt("bad Exp-Golomb code");
        return (hi << k) | get64(k);
    }

    Int64 getSigned()
    {
        UInt32 len = UInt32(get(7));
        if (len > 64)
            corrupt("bad signed number");
        if (!len)
            return 0;
        bool neg = get(1);
        UInt64 mag = get64(len);
        return neg ? Int64(UInt64(0) - mag) : Int64(mag);
    }

    size_t bytesConsumed() const { return size_t((pos + 7) >> 3); }
};

/// The length in bits of the Exp-Golomb code.
UInt32 expGolombLength(UInt64 value, UInt32 k)
{
    return 2 * bitLength((value >> k) + 1) - 1 + k;
}

/// A decode table entry packs: symbol (12 bits), number of bits to read (4), and the base of the next state
/// (16 bits), which includes the offset of the table of the context class of the symbol.
UInt32 packDecodeEntry(UInt32 sym, UInt32 nbits, UInt32 next)
{
    return sym | (nbits << 12) | (next << 16);
}

struct AnsEncodeSymbol
{
    UInt32 thresh = 0;   /// freq << max_bits
    Int32 delta = 0;    /// cumulative frequency before the symbol minus its frequency
    UInt8 max_bits = 0;
};

/// log2 of the integers up to the largest frequency plus one.
const double * log2Table()
{
    static std::vector<double> table = []
    {
        std::vector<double> t((1u << MAX_TABLE_LOG) + 2);
        t[0] = 0;
        for (UInt32 i = 1; i < t.size(); ++i)
            t[i] = std::log2(double(i));
        return t;
    }();
    return table.data();
}

using HeapItem = std::pair<double, UInt32>;

/// Replaces the top of a binary heap (the first element in the order of `before`) and restores the heap.
template <typename Before>
void replaceHeapTop(HeapItem * heap, size_t size, HeapItem item, Before before)
{
    size_t i = 0;
    while (true)
    {
        size_t child = 2 * i + 1;
        if (child >= size)
            break;
        if (child + 1 < size && before(heap[child + 1], heap[child]))
            ++child;
        if (!before(heap[child], item))
            break;
        heap[i] = heap[child];
        i = child;
    }
    heap[i] = item;
}

/// Scales the counts to frequencies with the sum of exactly 2^table_log; every present symbol gets at least 1.
/// Greedy by the change of the total code length, which is optimal for this separable convex objective.
void normalizeCounts(const UInt32 * counts, UInt32 alphabet, UInt32 table_log, UInt16 * freqs, std::vector<HeapItem> & heap)
{
    const UInt32 size = 1u << table_log;
    UInt64 total = 0;
    UInt32 present = 0;
    for (UInt32 s = 0; s < alphabet; ++s)
    {
        total += counts[s];
        present += counts[s] != 0;
    }
    if (!present)
        logicalError("empty histogram");
    if (present > size)
        logicalError("too many symbols for the table");

    Int64 sum = 0;
    for (UInt32 s = 0; s < alphabet; ++s)
    {
        if (!counts[s])
        {
            freqs[s] = 0;
            continue;
        }
        UInt64 f = UInt64(counts[s]) * size / total;
        if (f == 0)
            f = 1;
        freqs[s] = UInt16(f);
        sum += Int64(f);
    }
    Int64 diff = Int64(size) - sum;
    if (diff == 0)
        return;

    /// The frequency can reach 2^table_log (a single symbol), which does not fit UInt16 only for table_log == 16.
    const double * lg = log2Table();
    heap.clear();
    if (diff > 0)
    {
        /// The largest gain first.
        auto before = [](const HeapItem & a, const HeapItem & b) { return a > b; };
        auto gain = [&](UInt32 s) { return counts[s] * (lg[freqs[s] + 1] - lg[freqs[s]]); };
        for (UInt32 s = 0; s < alphabet; ++s)
            if (counts[s])
                heap.push_back({gain(s), s});
        /// The greedy choice is the `diff` largest of all the steps of all the symbols, and the steps of a symbol
        /// decrease. Most often they are the first steps of `diff` symbols: a selection finds them without a heap.
        const size_t steps = size_t(diff);
        if (steps <= heap.size())
        {
            if (steps < heap.size())
                std::nth_element(heap.begin(), heap.begin() + Int64(steps), heap.end(), before);
            const HeapItem last = *std::min_element(heap.begin(), heap.begin() + Int64(steps));
            bool first_steps_only = true;
            for (size_t i = 0; i < steps && first_steps_only; ++i)
            {
                UInt32 s = heap[i].second;
                first_steps_only = HeapItem{counts[s] * (lg[freqs[s] + 2] - lg[freqs[s] + 1]), s} < last;
            }
            if (first_steps_only)
            {
                for (size_t i = 0; i < steps; ++i)
                    ++freqs[heap[i].second];
                return;
            }
        }
        std::make_heap(heap.begin(), heap.end(), [](const HeapItem & a, const HeapItem & b) { return a < b; });
        while (diff > 0)
        {
            UInt32 s = heap[0].second;
            ++freqs[s];
            --diff;
            replaceHeapTop(heap.data(), heap.size(), {gain(s), s}, before);
        }
    }
    else
    {
        /// The smallest loss first.
        auto before = [](const HeapItem & a, const HeapItem & b) { return a < b; };
        auto loss = [&](UInt32 s) { return counts[s] * (lg[freqs[s]] - lg[freqs[s] - 1]); };
        for (UInt32 s = 0; s < alphabet; ++s)
            if (freqs[s] > 1)
                heap.push_back({loss(s), s});
        /// The same with the smallest losses, whose steps increase.
        const size_t steps = size_t(-diff);
        if (steps <= heap.size())
        {
            if (steps < heap.size())
                std::nth_element(heap.begin(), heap.begin() + Int64(steps), heap.end(), before);
            const HeapItem last = *std::max_element(heap.begin(), heap.begin() + Int64(steps));
            bool first_steps_only = true;
            for (size_t i = 0; i < steps && first_steps_only; ++i)
            {
                UInt32 s = heap[i].second;
                if (freqs[s] > 2)
                    first_steps_only = last < HeapItem{counts[s] * (lg[freqs[s] - 1] - lg[freqs[s] - 2]), s};
            }
            if (first_steps_only)
            {
                for (size_t i = 0; i < steps; ++i)
                    --freqs[heap[i].second];
                return;
            }
        }
        std::make_heap(heap.begin(), heap.end(), [](const HeapItem & a, const HeapItem & b) { return a > b; });
        size_t heap_size = heap.size();
        while (diff < 0)
        {
            if (!heap_size)
                logicalError("cannot normalize the histogram");
            UInt32 s = heap[0].second;
            --freqs[s];
            ++diff;
            if (freqs[s] > 1)
                replaceHeapTop(heap.data(), heap_size, {loss(s), s}, before);
            else
            {
                --heap_size;
                if (heap_size)
                    replaceHeapTop(heap.data(), heap_size, heap[heap_size], before);
            }
        }
    }
}

/// The cost in bits of coding the counts with the normalized frequencies.
double normalizedCost(const UInt32 * counts, const UInt16 * freqs, UInt32 alphabet, UInt32 table_log)
{
    const double * lg = log2Table();
    double bits = 0;
    for (UInt32 s = 0; s < alphabet; ++s)
        if (counts[s])
            bits += counts[s] * (double(table_log) - lg[freqs[s]]);
    return bits;
}

void spreadSymbols(const UInt16 * freqs, UInt32 alphabet, UInt32 table_log, UInt16 * spread)
{
    const UInt32 size = 1u << table_log;
    const UInt32 mask = size - 1;
    const UInt32 step = (size >> 1) + (size >> 3) + 3;
    UInt32 pos = 0;
    for (UInt32 s = 0; s < alphabet; ++s)
    {
        for (UInt32 j = 0; j < freqs[s]; ++j)
        {
            spread[pos] = UInt16(s);
            pos = (pos + step) & mask;
        }
    }
}

/// `symbol_ctx` (may be null) gives the context class of every symbol; it is embedded into the entries so that
/// the decoder gets the context of the next token without another lookup.
void buildDecodeTable(const UInt16 * freqs, UInt32 alphabet, UInt32 table_log, const UInt8 * symbol_ctx, UInt32 * table)
{
    const UInt32 size = 1u << table_log;
    UInt16 spread[1u << MAX_TABLE_LOG];
    UInt16 next[MAX_ALPHABET];
    spreadSymbols(freqs, alphabet, table_log, spread);
    for (UInt32 s = 0; s < alphabet; ++s)
        next[s] = freqs[s];
    for (UInt32 x = 0; x < size; ++x)
    {
        UInt32 s = spread[x];
        UInt32 k = next[s]++;
        UInt32 nbits = table_log - (31 - std::countl_zero(k));
        UInt32 base = (k << nbits) - size;
        table[x] = packDecodeEntry(s, nbits, ((symbol_ctx ? UInt32(symbol_ctx[s]) : 0) << table_log) + base);
    }
}

struct AnsEncodeTable
{
    std::vector<AnsEncodeSymbol> symbols;
    std::vector<UInt16> states;
    UInt32 table_log = 0;

    void build(const UInt16 * freqs, UInt32 alphabet, UInt32 table_log_)
    {
        table_log = table_log_;
        const UInt32 size = 1u << table_log;
        symbols.assign(alphabet, {});
        states.assign(size, 0);
        std::vector<UInt32> cum(alphabet + 1, 0);
        for (UInt32 s = 0; s < alphabet; ++s)
            cum[s + 1] = cum[s] + freqs[s];
        UInt16 spread[1u << MAX_TABLE_LOG];
        spreadSymbols(freqs, alphabet, table_log, spread);
        std::vector<UInt32> fill(cum.begin(), cum.end() - 1);
        for (UInt32 x = 0; x < size; ++x)
            states[fill[spread[x]]++] = UInt16(x);
        for (UInt32 s = 0; s < alphabet; ++s)
        {
            UInt32 f = freqs[s];
            if (!f)
                continue;
            UInt32 max_bits = table_log - (31 - std::countl_zero(f));
            symbols[s].max_bits = UInt8(max_bits);
            symbols[s].thresh = f << max_bits;
            symbols[s].delta = Int32(cum[s]) - Int32(f);
        }
    }

    /// One step of encoding: returns the bits to output; `state` is in [size, 2 * size).
    inline void encode(UInt32 & state, UInt32 sym, UInt16 & bits, UInt8 & nbits) const
    {
        const AnsEncodeSymbol & e = symbols[sym];
        UInt32 nb = e.max_bits - (state < e.thresh);
        bits = UInt16(state & ((1u << nb) - 1));
        nbits = UInt8(nb);
        state = (1u << table_log) + states[(state >> nb) + e.delta];
    }
};

/// Numbering of the logarithmic classes for the given (split, msb): magnitudes below 2^split are classes
/// themselves; above, the class is the exponent and the `msb` bits below the leading one.
struct ClassLayout
{
    UInt32 split = 0;
    UInt32 exp_base[64] = {};
    UInt8 exp_msb[64] = {};
    UInt32 num_classes = 0;

    void init(UInt32 split_, UInt32 msb)
    {
        split = split_;
        UInt32 cid = 1u << split;
        for (UInt32 e = 0; e < 64; ++e)
        {
            if (e < split)
            {
                exp_base[e] = 0;
                exp_msb[e] = 0;
                continue;
            }
            UInt32 m = msb;
            if (e > MAX_RAW_BITS + m)
                m = e - MAX_RAW_BITS;
            exp_base[e] = cid;
            exp_msb[e] = UInt8(m);
            cid += 1u << m;
        }
        num_classes = cid;
    }

    inline UInt32 classOf(UInt64 mag) const
    {
        if (mag < (UInt64(1) << split))
            return UInt32(mag);
        UInt32 e = 63 - std::countl_zero(mag);
        UInt32 m = exp_msb[e];
        return exp_base[e] + UInt32((mag >> (e - m)) & ((UInt64(1) << m) - 1));
    }

    /// Returns false if the class id is out of range.
    bool classRange(UInt32 cid, UInt64 & mag_base, UInt32 & nbits) const
    {
        if (cid < (1u << split))
        {
            mag_base = cid;
            nbits = 0;
            return true;
        }
        if (cid >= num_classes)
            return false;
        UInt32 e = split;
        while (e < 63 && exp_base[e + 1] <= cid)
            ++e;
        UInt32 m = exp_msb[e];
        UInt32 top = cid - exp_base[e];
        nbits = e - m;
        mag_base = (UInt64(1) << e) | (UInt64(top) << nbits);
        return true;
    }
};

constexpr UInt32 MAX_SPLIT = 6;
constexpr UInt32 MAX_MSB = 4;
constexpr UInt32 MAX_CONTEXTS = 3;
constexpr UInt32 MAX_FIXED_BINS = 4096;

struct StreamModel
{
    bool fixed = false;
    UInt8 split = 0;
    UInt8 msb = 0;
    UInt8 shift = 0;
    Int64 fmin = 0;
    bool runs = false;
    UInt16 run_mask = 0;
    std::vector<Int64> dict;
    std::vector<UInt32> bin_ids;
    UInt8 nctx = 1;
    UInt8 thresholds[MAX_CONTEXTS - 1] = {};
    UInt8 table_log = MIN_TABLE_LOG;
    std::vector<UInt16> freqs;   /// nctx * alphabet

    UInt32 alphabet() const { return UInt32(dict.size() + bin_ids.size()) + std::popcount(run_mask); }
};

/// The bins of the alphabet, as needed for decoding and for the context classes.
struct TokenTable
{
    Int64 lo[MAX_ALPHABET];
    UInt8 nbits[MAX_ALPHABET];
    UInt8 ctx[MAX_ALPHABET];
    UInt32 alphabet = 0;
    UInt32 first_run = 0;

    /// Returns false on an invalid model.
    bool build(const StreamModel & m)
    {
        alphabet = m.alphabet();
        if (alphabet == 0 || alphabet > MAX_ALPHABET)
            return false;
        UInt32 t = 0;
        UInt8 magbits[MAX_ALPHABET];
        for (Int64 v : m.dict)
        {
            lo[t] = v;
            nbits[t] = 0;
            magbits[t] = UInt8(bitLength(magnitude(v)));
            ++t;
        }
        if (m.fixed)
        {
            if (m.shift > MAX_RAW_BITS)
                return false;
            for (UInt32 id : m.bin_ids)
            {
                if (id >= MAX_FIXED_BINS)
                    return false;
                Int64 l = Int64(UInt64(m.fmin) + (UInt64(id) << m.shift));
                lo[t] = l;
                nbits[t] = m.shift;
                Int64 h = Int64(UInt64(l) + lowMask(m.shift));
                magbits[t] = UInt8(std::max(bitLength(magnitude(l)), bitLength(magnitude(h))));
                ++t;
            }
        }
        else
        {
            if (m.msb > m.split || m.split > 7)
                return false;
            ClassLayout layout;
            layout.init(m.split, m.msb);
            for (UInt32 id : m.bin_ids)
            {
                UInt64 mag_base;
                UInt32 nb;
                if (!layout.classRange(id >> 1, mag_base, nb))
                    return false;
                bool neg = id & 1;
                if (neg && (id >> 1) == 0)
                    return false;
                lo[t] = neg ? Int64(UInt64(0) - (mag_base + lowMask(nb))) : Int64(mag_base);
                nbits[t] = UInt8(nb);
                magbits[t] = UInt8(bitLength(mag_base + lowMask(nb)));
                ++t;
            }
        }
        first_run = t;
        for (UInt32 e = 0; e <= MAX_RUN_EXP; ++e)
        {
            if (m.run_mask & (1u << e))
            {
                lo[t] = 0;
                nbits[t] = UInt8(e);
                magbits[t] = 0;
                ++t;
            }
        }
        for (UInt32 i = 0; i < alphabet; ++i)
        {
            UInt8 c = 0;
            for (UInt32 k = 0; k + 1 < m.nctx; ++k)
                c += magbits[i] > m.thresholds[k];
            ctx[i] = c;
        }
        return true;
    }
};

/// The best order (up to max_k) of the Exp-Golomb code for a set of numbers, from the histogram of their bit
/// lengths (65 entries), and the approximate size with it: a number of b bits takes about
/// 2 * max(b - k, 0) + 1 + k bits with the order k.
UInt32 bestExpGolombOrder(const UInt32 * lengths, UInt32 max_k, UInt64 & best_bits)
{
    /// An order above the longest number only adds bits.
    UInt32 top = 64;
    while (top && !lengths[top])
        --top;
    UInt32 best_k = 0;
    best_bits = ~UInt64(0);
    for (UInt32 k = 0; k <= std::min(max_k, top); ++k)
    {
        UInt64 bits = 0;
        for (UInt32 b = 0; b <= top; ++b)
            bits += UInt64(lengths[b]) * (2 * (b > k ? b - k : 0) + 1 + k);
        if (bits < best_bits)
        {
            best_bits = bits;
            best_k = k;
        }
    }
    return best_k;
}

/// The approximate size of the frequencies of one context in the header of the model, as `writeModel` codes them.
UInt64 frequencyHeaderBits(const UInt16 * f, UInt32 alphabet)
{
    UInt32 lengths[65] = {};
    for (UInt32 t = 0; t + 1 < alphabet; ++t)
        ++lengths[bitLength(f[t])];
    UInt64 exp_golomb = 0;
    bestExpGolombOrder(lengths, 12, exp_golomb);
    UInt64 length_prefixed = 0;
    for (UInt32 b = 0; b <= MAX_TABLE_LOG + 1; ++b)
        length_prefixed += UInt64(lengths[b]) * (4 + (b > 1 ? b - 1 : 0));
    return std::min(exp_golomb, length_prefixed) + 4;
}

void writeModel(BitWriter & w, const StreamModel & m)
{
    w.put(m.fixed, 1);
    if (m.fixed)
    {
        w.put(m.shift, 6);
        w.putSigned(m.fmin);
    }
    else
    {
        w.put(m.split, 3);
        w.put(m.msb, 3);
    }

    auto bestOrder = [](const UInt32 * lengths, UInt32 max_k)
    {
        UInt64 unused_bits = 0;
        return bestExpGolombOrder(lengths, max_k, unused_bits);
    };

    w.put(m.dict.size(), 13);
    if (!m.dict.empty())
    {
        UInt32 lengths[65] = {};
        for (size_t i = 1; i < m.dict.size(); ++i)
            ++lengths[bitLength(UInt64(m.dict[i]) - UInt64(m.dict[i - 1]) - 1)];
        UInt32 kd = bestOrder(lengths, 60);
        w.put(kd, 6);
        w.putSigned(m.dict[0]);
        for (size_t i = 1; i < m.dict.size(); ++i)
            w.putExpGolomb(UInt64(m.dict[i]) - UInt64(m.dict[i - 1]) - 1, kd);
    }

    w.put(m.bin_ids.size(), 13);
    if (!m.bin_ids.empty())
    {
        UInt32 lengths[65] = {};
        ++lengths[bitLength(m.bin_ids[0])];
        for (size_t i = 1; i < m.bin_ids.size(); ++i)
            ++lengths[bitLength(m.bin_ids[i] - m.bin_ids[i - 1] - 1)];
        UInt32 kb = bestOrder(lengths, 12);
        w.put(kb, 4);
        w.putExpGolomb(m.bin_ids[0], kb);
        for (size_t i = 1; i < m.bin_ids.size(); ++i)
            w.putExpGolomb(m.bin_ids[i] - m.bin_ids[i - 1] - 1, kb);
    }

    w.put(m.runs, 1);
    if (m.runs)
        w.put(m.run_mask, MAX_RUN_EXP + 1);

    w.put(m.nctx - 1, 2);
    for (UInt32 k = 0; k + 1 < m.nctx; ++k)
        w.put(m.thresholds[k], 7);

    w.put(m.table_log - MIN_TABLE_LOG, 3);
    UInt32 alphabet = m.alphabet();
    for (UInt32 c = 0; c < m.nctx; ++c)
    {
        const UInt16 * f = m.freqs.data() + size_t(c) * alphabet;
        /// The last frequency is implied by the sum. The others: Exp-Golomb codes of the best order, or
        /// (mode 15) a 4-bit length followed by the bits below the leading one, which suits a wide spread.
        if (alphabet == 1)
            continue;
        UInt32 lengths[65] = {};
        for (UInt32 t = 0; t + 1 < alphabet; ++t)
            ++lengths[bitLength(f[t])];
        UInt32 kf = bestOrder(lengths, 12);
        UInt64 eg_bits = 0;
        UInt64 lp_bits = 0;
        for (UInt32 s = 0; s + 1 < alphabet; ++s)
        {
            eg_bits += expGolombLength(f[s], kf);
            UInt32 len = bitLength(f[s]);
            lp_bits += 4 + (len > 1 ? len - 1 : 0);
        }
        if (lp_bits < eg_bits)
        {
            w.put(15, 4);
            for (UInt32 s = 0; s + 1 < alphabet; ++s)
            {
                UInt32 len = bitLength(f[s]);
                w.put(len, 4);
                if (len > 1)
                    w.put(f[s] & lowMask(len - 1), len - 1);
            }
        }
        else
        {
            w.put(kf, 4);
            for (UInt32 s = 0; s + 1 < alphabet; ++s)
                w.putExpGolomb(f[s], kf);
        }
    }
}

void readModel(BitReader & r, StreamModel & m)
{
    m.fixed = r.get(1);
    if (m.fixed)
    {
        m.shift = UInt8(r.get(6));
        m.fmin = r.getSigned();
    }
    else
    {
        m.split = UInt8(r.get(3));
        m.msb = UInt8(r.get(3));
    }

    UInt32 nd = UInt32(r.get(13));
    m.dict.resize(nd);
    if (nd)
    {
        UInt32 kd = UInt32(r.get(6));
        m.dict[0] = r.getSigned();
        for (UInt32 i = 1; i < nd; ++i)
            m.dict[i] = Int64(UInt64(m.dict[i - 1]) + r.getExpGolomb(kd) + 1);
    }

    UInt32 nb = UInt32(r.get(13));
    m.bin_ids.resize(nb);
    if (nb)
    {
        UInt32 kb = UInt32(r.get(4));
        UInt64 id = r.getExpGolomb(kb);
        for (UInt32 i = 0; i < nb; ++i)
        {
            if (i)
                id += r.getExpGolomb(kb) + 1;
            if (id >= (UInt64(1) << 20))
                corrupt("bad bin id");
            m.bin_ids[i] = UInt32(id);
        }
    }

    m.runs = r.get(1);
    m.run_mask = m.runs ? UInt16(r.get(MAX_RUN_EXP + 1)) : 0;

    m.nctx = UInt8(r.get(2) + 1);
    if (m.nctx > MAX_CONTEXTS)
        corrupt("too many contexts");
    for (UInt32 k = 0; k + 1 < m.nctx; ++k)
        m.thresholds[k] = UInt8(r.get(7));

    m.table_log = UInt8(r.get(3) + MIN_TABLE_LOG);
    UInt32 alphabet = m.alphabet();
    if (alphabet == 0 || alphabet > MAX_ALPHABET || alphabet > (1u << m.table_log))
        corrupt("bad alphabet size");
    m.freqs.resize(size_t(m.nctx) * alphabet);
    for (UInt32 c = 0; c < m.nctx; ++c)
    {
        const UInt32 size = 1u << m.table_log;
        UInt16 * f = m.freqs.data() + size_t(c) * alphabet;
        UInt64 sum = 0;
        if (alphabet > 1)
        {
            UInt32 kf = UInt32(r.get(4));
            if (kf > 12 && kf != 15)
                corrupt("bad frequency coding");
            for (UInt32 s = 0; s + 1 < alphabet; ++s)
            {
                UInt64 value;
                if (kf == 15)
                {
                    UInt32 len = UInt32(r.get(4));
                    value = len > 1 ? (UInt64(1) << (len - 1)) | r.get(len - 1) : len;
                }
                else
                    value = r.getExpGolomb(kf);
                if (value > size)
                    corrupt("bad frequency");
                f[s] = UInt16(value);
                sum += value;
            }
        }
        if (sum > size)
            corrupt("frequencies exceed the table size");
        f[alphabet - 1] = UInt16(size - sum);
    }
}

/// x * log2(x) for small integers.
const double * xlog2xTable()
{
    static std::vector<double> table = []
    {
        std::vector<double> t(CHUNK_MAX + 2);
        t[0] = 0;
        for (UInt32 i = 1; i < t.size(); ++i)
            t[i] = i * std::log2(double(i));
        return t;
    }();
    return table.data();
}

struct StreamDecoder
{
    StreamModel model;
    TokenTable table;
    Scratch<UInt32> dec;      /// nctx << table_log
    /// Entries that also carry the bin of the symbol: next state (16 bits), bits of the token (4),
    /// raw bits (6), the lower bound of the bin as a signed 32-bit number (in the upper half).
    Scratch<UInt64> fat;
    bool use_fat = false;
    Int64 extra = 0;             /// added to the values of the fat entries: they hold the bins without the center
    Scratch<UInt16> tok;
    UInt32 ntok = 0;
    UInt32 ans_bytes = 0;
    UInt32 raw_bytes = 0;
    UInt32 max_nbits = 0;
    Int64 fill_value = 0;

    /// The position in a stream without runs that is decoded in pieces.
    struct Cursor
    {
        const UInt8 * ans = nullptr;
        const UInt8 * raws = nullptr;
        UInt64 ans_limit = 0;
        UInt64 raw_limit = 0;
        UInt64 apos = 0;
        UInt64 rpos = 0;
        UInt32 x[ANS_LANES] = {};
        UInt32 done = 0;
    };

    /// Parses the model and builds the tables. `add` is added to every decoded value (the center of the residuals).
    void parse(BitReader & header, Int64 add)
    {
        readModel(header, model);
        ntok = UInt32(header.get(14)) + 1;
        ans_bytes = UInt32(header.get(16));
        raw_bytes = UInt32(header.get(18));
        if (!table.build(model))
            corrupt("invalid model");
        UInt32 alphabet = table.alphabet;
        max_nbits = 0;
        bool narrow = true;
        for (UInt32 t = 0; t < table.first_run; ++t)
        {
            narrow &= table.lo[t] == Int64(Int32(table.lo[t]));
            max_nbits = std::max<UInt32>(max_nbits, table.nbits[t]);
        }
        fill_value = add;
        const UInt32 size = 1u << model.table_log;
        dec.resize(size_t(model.nctx) * size);
        for (UInt32 c = 0; c < model.nctx; ++c)
            buildDecodeTable(model.freqs.data() + size_t(c) * alphabet, alphabet, model.table_log,
                model.nctx > 1 ? table.ctx : nullptr, dec.data() + size_t(c) * size);

        use_fat = narrow && !model.runs && alphabet > 1 && max_nbits > 0;
        extra = 0;
        if (use_fat)
        {
            extra = add;
            const size_t entries = size_t(model.nctx) * size;
            const UInt32 * thin = dec.data();
            UInt64 * entry = fat.resize(entries);
            for (size_t x = 0; x < entries; ++x)
            {
                UInt32 e = thin[x];
                UInt32 s = e & 0xfff;
                entry[x] = UInt64(e >> 16) | (UInt64((e >> 12) & 15) << 16) | (UInt64(table.nbits[s]) << 20)
                    | (UInt64(UInt32(Int32(table.lo[s]))) << 32);
            }
        }
        else
        {
            for (UInt32 t = 0; t < table.first_run; ++t)
                table.lo[t] = Int64(UInt64(table.lo[t]) + UInt64(add));
        }
    }

    size_t payloadSize() const { return size_t(ans_bytes) + raw_bytes; }
    bool hasRuns() const { return model.runs; }

    /// Adds one more constant to every decoded value.
    void setExtra(Int64 value)
    {
        if (!value)
            return;
        if (use_fat)
            extra = Int64(UInt64(extra) + UInt64(value));
        else
        {
            for (UInt32 t = 0; t < table.first_run; ++t)
                table.lo[t] = Int64(UInt64(table.lo[t]) + UInt64(value));
        }
        fill_value = Int64(UInt64(fill_value) + UInt64(value));
    }

    void begin(const UInt8 * payload, Cursor & c) const
    {
        const UInt32 table_log = model.table_log;
        c.ans = payload;
        c.raws = payload + ans_bytes;
        c.ans_limit = UInt64(ans_bytes) * 8;
        c.raw_limit = UInt64(raw_bytes) * 8;
        if (UInt64(ANS_LANES) * table_log > c.ans_limit)
            corrupt("truncated token stream");
        UInt64 w = loadLE64(payload);
        for (UInt32 lane = 0; lane < ANS_LANES; ++lane)
        {
            c.x[lane] = UInt32(w & lowMask(table_log));
            w >>= table_log;
        }
        c.apos = UInt64(ANS_LANES) * table_log;
        c.rpos = 0;
        c.done = 0;
    }

    /// Decodes the next `count` values of a stream without runs: tokens and raw bits in one loop.
    /// `count` must be a multiple of the number of lanes except in the last call.
    void next(Cursor & c, Int64 * out, UInt32 count) const
    {
        if (c.done + count > ntok)
            corrupt("wrong number of tokens");
        const UInt32 * d = dec.data();
        const Int64 * lo = table.lo;
        const UInt8 * nbv = table.nbits;
        const UInt8 * ans = c.ans;
        const UInt8 * raws = c.raws;
        const UInt64 ans_limit = c.ans_limit;
        const UInt64 raw_limit = c.raw_limit;
        UInt64 apos = c.apos;
        UInt64 rpos = c.rpos;
        UInt32 x0 = c.x[0], x1 = c.x[1], x2 = c.x[2], x3 = c.x[3];
        UInt32 i = 0;
        if (table.alphabet == 1)
        {
            /// A single symbol takes no bits in the token stream.
            UInt32 nb = nbv[0];
            UInt64 l = UInt64(lo[0]);
            for (; i < count; ++i)
            {
                UInt64 w = loadLE64(raws + (rpos >> 3)) >> (rpos & 7);
                rpos += nb;
                if (rpos > raw_limit)
                    corrupt("truncated raw bits");
                out[i] = Int64(l + (w & ((UInt64(1) << nb) - 1)));
            }
        }
        else if (max_nbits == 0)
        {
            for (; i + 4 <= count; i += 4)
            {
                if (apos > ans_limit)
                    corrupt("truncated token stream");
                UInt64 w = loadLE64(ans + (apos >> 3)) >> (apos & 7);
                UInt32 e0 = d[x0], e1 = d[x1], e2 = d[x2], e3 = d[x3];
                UInt32 n0 = (e0 >> 12) & 15, n1 = (e1 >> 12) & 15, n2 = (e2 >> 12) & 15, n3 = (e3 >> 12) & 15;
                out[i] = lo[e0 & 0xfff];
                out[i + 1] = lo[e1 & 0xfff];
                out[i + 2] = lo[e2 & 0xfff];
                out[i + 3] = lo[e3 & 0xfff];
                x0 = (e0 >> 16) + UInt32(w & ((1u << n0) - 1));
                w >>= n0;
                x1 = (e1 >> 16) + UInt32(w & ((1u << n1) - 1));
                w >>= n1;
                x2 = (e2 >> 16) + UInt32(w & ((1u << n2) - 1));
                w >>= n2;
                x3 = (e3 >> 16) + UInt32(w & ((1u << n3) - 1));
                apos += n0 + n1 + n2 + n3;
            }
        }
        else if (use_fat)
        {
            /// Four values take at most 224 raw bits: no checks of the raw position while that much is left.
            const UInt64 * f = fat.data();
            const UInt64 add = UInt64(extra);
            for (; i + 4 <= count && rpos + 4 * MAX_RAW_BITS <= raw_limit; i += 4)
            {
                if (apos > ans_limit)
                    corrupt("truncated token stream");
                UInt64 w = loadLE64(ans + (apos >> 3)) >> (apos & 7);
                UInt64 e0 = f[x0], e1 = f[x1], e2 = f[x2], e3 = f[x3];
                UInt32 n0 = UInt32(e0 >> 16) & 15, n1 = UInt32(e1 >> 16) & 15, n2 = UInt32(e2 >> 16) & 15, n3 = UInt32(e3 >> 16) & 15;
                x0 = UInt32(e0 & 0xffff) + UInt32(w & ((1u << n0) - 1));
                w >>= n0;
                x1 = UInt32(e1 & 0xffff) + UInt32(w & ((1u << n1) - 1));
                w >>= n1;
                x2 = UInt32(e2 & 0xffff) + UInt32(w & ((1u << n2) - 1));
                w >>= n2;
                x3 = UInt32(e3 & 0xffff) + UInt32(w & ((1u << n3) - 1));
                apos += n0 + n1 + n2 + n3;

                UInt32 r0 = UInt32(e0 >> 20) & 63, r1 = UInt32(e1 >> 20) & 63, r2 = UInt32(e2 >> 20) & 63, r3 = UInt32(e3 >> 20) & 63;
                UInt64 p1 = rpos + r0, p2 = p1 + r1, p3 = p2 + r2;
                UInt64 w0 = loadLE64(raws + (rpos >> 3)) >> (rpos & 7);
                UInt64 w1 = loadLE64(raws + (p1 >> 3)) >> (p1 & 7);
                UInt64 w2 = loadLE64(raws + (p2 >> 3)) >> (p2 & 7);
                UInt64 w3 = loadLE64(raws + (p3 >> 3)) >> (p3 & 7);
                rpos = p3 + r3;
                out[i] = Int64(add + UInt64(Int64(Int32(e0 >> 32))) + (w0 & ((UInt64(1) << r0) - 1)));
                out[i + 1] = Int64(add + UInt64(Int64(Int32(e1 >> 32))) + (w1 & ((UInt64(1) << r1) - 1)));
                out[i + 2] = Int64(add + UInt64(Int64(Int32(e2 >> 32))) + (w2 & ((UInt64(1) << r2) - 1)));
                out[i + 3] = Int64(add + UInt64(Int64(Int32(e3 >> 32))) + (w3 & ((UInt64(1) << r3) - 1)));
            }
        }
        else
        {
            /// Four values take at most 224 raw bits: no checks of the raw position while that much is left.
            for (; i + 4 <= count && rpos + 4 * MAX_RAW_BITS <= raw_limit; i += 4)
            {
                if (apos > ans_limit)
                    corrupt("truncated token stream");
                UInt64 w = loadLE64(ans + (apos >> 3)) >> (apos & 7);
                UInt32 e0 = d[x0], e1 = d[x1], e2 = d[x2], e3 = d[x3];
                UInt32 n0 = (e0 >> 12) & 15, n1 = (e1 >> 12) & 15, n2 = (e2 >> 12) & 15, n3 = (e3 >> 12) & 15;
                UInt32 s0 = e0 & 0xfff, s1 = e1 & 0xfff, s2 = e2 & 0xfff, s3 = e3 & 0xfff;
                x0 = (e0 >> 16) + UInt32(w & ((1u << n0) - 1));
                w >>= n0;
                x1 = (e1 >> 16) + UInt32(w & ((1u << n1) - 1));
                w >>= n1;
                x2 = (e2 >> 16) + UInt32(w & ((1u << n2) - 1));
                w >>= n2;
                x3 = (e3 >> 16) + UInt32(w & ((1u << n3) - 1));
                apos += n0 + n1 + n2 + n3;

                UInt32 r0 = nbv[s0], r1 = nbv[s1], r2 = nbv[s2], r3 = nbv[s3];
                UInt64 p1 = rpos + r0, p2 = p1 + r1, p3 = p2 + r2;
                UInt64 w0 = loadLE64(raws + (rpos >> 3)) >> (rpos & 7);
                UInt64 w1 = loadLE64(raws + (p1 >> 3)) >> (p1 & 7);
                UInt64 w2 = loadLE64(raws + (p2 >> 3)) >> (p2 & 7);
                UInt64 w3 = loadLE64(raws + (p3 >> 3)) >> (p3 & 7);
                rpos = p3 + r3;
                out[i] = Int64(UInt64(lo[s0]) + (w0 & ((UInt64(1) << r0) - 1)));
                out[i + 1] = Int64(UInt64(lo[s1]) + (w1 & ((UInt64(1) << r1) - 1)));
                out[i + 2] = Int64(UInt64(lo[s2]) + (w2 & ((UInt64(1) << r2) - 1)));
                out[i + 3] = Int64(UInt64(lo[s3]) + (w3 & ((UInt64(1) << r3) - 1)));
            }
        }
        /// The tail, and the end of the raw bits: one value at a time with all the checks.
        if (i < count)
        {
            UInt32 x[ANS_LANES] = {x0, x1, x2, x3};
            for (; i < count; ++i)
            {
                if (apos > ans_limit)
                    corrupt("truncated token stream");
                UInt32 lane = (c.done + i) & (ANS_LANES - 1);
                UInt64 w = loadLE64(ans + (apos >> 3)) >> (apos & 7);
                UInt32 e = d[x[lane]];
                UInt32 nb = (e >> 12) & 15;
                UInt32 s = e & 0xfff;
                x[lane] = (e >> 16) + UInt32(w & ((1u << nb) - 1));
                apos += nb;
                UInt32 r = nbv[s];
                UInt64 wr = loadLE64(raws + (rpos >> 3)) >> (rpos & 7);
                rpos += r;
                if (rpos > raw_limit)
                    corrupt("truncated raw bits");
                out[i] = Int64(UInt64(lo[s]) + UInt64(extra) + (wr & ((UInt64(1) << r) - 1)));
            }
            x0 = x[0]; x1 = x[1]; x2 = x[2]; x3 = x[3];
        }
        c.apos = apos;
        c.rpos = rpos;
        c.x[0] = x0; c.x[1] = x1; c.x[2] = x2; c.x[3] = x3;
        c.done += count;
    }

    void finish(const Cursor & c) const
    {
        if (c.done != ntok)
            corrupt("wrong number of tokens");
        if (c.apos > c.ans_limit)
            corrupt("truncated token stream");
        const UInt32 mask = (1u << model.table_log) - 1;
        if (table.alphabet > 1 && ((c.x[0] | c.x[1] | c.x[2] | c.x[3]) & mask))
            corrupt("inconsistent final state of the token stream");
    }

    /// Decodes the tokens into `tok`. The payload must have 8 readable bytes after its end.
    void decodeTokens(const UInt8 * payload, UInt32 n)
    {
        if (ntok > n)
            corrupt("too many tokens");
        UInt16 * t = tok.resize(ntok + 4);
        if (table.alphabet == 1)
        {
            std::fill(t, t + ntok, UInt16(0));
            return;
        }
        Cursor c;
        begin(payload, c);
        const UInt32 * d = dec.data();
        const UInt64 ans_limit = c.ans_limit;
        UInt64 apos = c.apos;
        UInt32 x0 = c.x[0], x1 = c.x[1], x2 = c.x[2], x3 = c.x[3];
        UInt32 i = 0;
        for (; i + 4 <= ntok; i += 4)
        {
            if (apos > ans_limit)
                corrupt("truncated token stream");
            UInt64 w = loadLE64(payload + (apos >> 3)) >> (apos & 7);
            UInt32 e0 = d[x0], e1 = d[x1], e2 = d[x2], e3 = d[x3];
            UInt32 n0 = (e0 >> 12) & 15, n1 = (e1 >> 12) & 15, n2 = (e2 >> 12) & 15, n3 = (e3 >> 12) & 15;
            t[i] = UInt16(e0 & 0xfff);
            t[i + 1] = UInt16(e1 & 0xfff);
            t[i + 2] = UInt16(e2 & 0xfff);
            t[i + 3] = UInt16(e3 & 0xfff);
            x0 = (e0 >> 16) + UInt32(w & ((1u << n0) - 1));
            w >>= n0;
            x1 = (e1 >> 16) + UInt32(w & ((1u << n1) - 1));
            w >>= n1;
            x2 = (e2 >> 16) + UInt32(w & ((1u << n2) - 1));
            w >>= n2;
            x3 = (e3 >> 16) + UInt32(w & ((1u << n3) - 1));
            apos += n0 + n1 + n2 + n3;
        }
        UInt32 x[ANS_LANES] = {x0, x1, x2, x3};
        for (; i < ntok; ++i)
        {
            if (apos > ans_limit)
                corrupt("truncated token stream");
            UInt64 w = loadLE64(payload + (apos >> 3)) >> (apos & 7);
            UInt32 e = d[x[i & 3]];
            UInt32 nb = (e >> 12) & 15;
            t[i] = UInt16(e & 0xfff);
            x[i & 3] = (e >> 16) + UInt32(w & ((1u << nb) - 1));
            apos += nb;
        }
        if (apos > ans_limit)
            corrupt("truncated token stream");
        const UInt32 mask = (1u << model.table_log) - 1;
        if ((x[0] | x[1] | x[2] | x[3]) & mask)
            corrupt("inconsistent final state of the token stream");
    }

    /// Decodes exactly n values of a stream with runs. The payload must have 8 readable bytes after its end.
    void decodeRuns(const UInt8 * payload, UInt32 n, Int64 * out)
    {
        decodeTokens(payload, n);
        const UInt16 * t = tok.data();
        const UInt8 * rp = payload + ans_bytes;
        const UInt64 raw_limit = UInt64(raw_bytes) * 8;
        UInt64 rpos = 0;
        const Int64 * lo = table.lo;
        const UInt8 * nbv = table.nbits;
        const UInt32 first_run = table.first_run;
        UInt32 j = 0;
        for (UInt32 i = 0; i < ntok; ++i)
        {
            UInt32 s = t[i];
            UInt32 nb = nbv[s];
            UInt64 w = loadLE64(rp + (rpos >> 3)) >> (rpos & 7);
            rpos += nb;
            if (rpos > raw_limit)
                corrupt("truncated raw bits");
            UInt64 bits = w & ((UInt64(1) << nb) - 1);
            if (s < first_run)
            {
                if (j >= n)
                    corrupt("too many values");
                out[j++] = Int64(UInt64(lo[s]) + bits);
            }
            else
            {
                UInt32 len = (1u << nb) + UInt32(bits);
                if (len > n - j)
                    corrupt("too many values");
                std::fill(out + j, out + j + len, fill_value);
                j += len;
            }
        }
        if (j != n)
            corrupt("wrong number of values");
    }

    /// Decodes exactly n values in one go.
    void decode(const UInt8 * payload, UInt32 n, Int64 * out)
    {
        if (model.runs)
        {
            decodeRuns(payload, n, out);
            return;
        }
        if (ntok != n)
            corrupt("wrong number of tokens");
        Cursor c;
        begin(payload, c);
        next(c, out, n);
        finish(c);
    }

    /// Decodes n values and calls emit(index, value) for the values that differ from the fill value.
    /// For streams that are mostly zero runs it takes time proportional to the number of tokens.
    template <typename Emit>
    void decodeSparse(const UInt8 * payload, UInt32 n, Emit && emit)
    {
        decodeTokens(payload, n);
        const UInt16 * t = tok.data();
        const UInt8 * rp = payload + ans_bytes;
        const UInt64 raw_limit = UInt64(raw_bytes) * 8;
        UInt64 rpos = 0;
        const Int64 * lo = table.lo;
        const UInt8 * nbv = table.nbits;
        const UInt32 first_run = table.first_run;
        UInt32 j = 0;
        for (UInt32 i = 0; i < ntok; ++i)
        {
            UInt32 s = t[i];
            UInt32 nb = nbv[s];
            UInt64 w = loadLE64(rp + (rpos >> 3)) >> (rpos & 7);
            rpos += nb;
            if (rpos > raw_limit)
                corrupt("truncated raw bits");
            UInt64 bits = w & ((UInt64(1) << nb) - 1);
            if (s < first_run)
            {
                if (j >= n)
                    corrupt("too many values");
                Int64 v = Int64(UInt64(lo[s]) + bits);
                if (v != fill_value)
                    emit(j, v);
                ++j;
            }
            else
            {
                UInt32 len = (1u << nb) + UInt32(bits);
                if (len > n - j)
                    corrupt("too many values");
                j += len;
            }
        }
        if (j != n)
            corrupt("wrong number of values");
    }
};

struct StreamEncoder
{
    /// --- Analysis.
    const Int64 * data = nullptr;
    UInt32 n = 0;
    ClassLayout fine;
    std::vector<UInt32> fine_hist;
    std::vector<UInt32> fine_used;
    std::vector<UInt32> fixed_hist;
    ClassLayout layout;
    double estimated_bits = 0;       /// the payload: estimated by `analyze`, exact after `complete`
    double counted_bits = 0;         /// the payload and the model: estimated by `count` from the counts of the tokens

    /// --- Tokens.
    StreamModel model;
    TokenTable table;
    std::vector<UInt16> tok;
    std::vector<UInt64> raw;
    std::vector<UInt8> rawbits;
    std::vector<UInt32> token_counts;
    std::vector<UInt32> ctx_counts;
    UInt32 ntok = 0;

    /// --- Scratch.
    std::vector<UInt32> prov;
    std::vector<UInt32> bin_counts;
    std::vector<std::pair<Int64, UInt32>> promoted;
    std::vector<UInt64> hash_keys;
    std::vector<UInt32> hash_counts;
    std::vector<UInt32> hash_slots;
    Scratch<UInt8> model_bytes;
    UInt64 model_bits = 0;
    std::vector<UInt32> slots;
    std::vector<UInt16> slot_tok;
    bool dict_counted = false;       /// `slots` and `slot_tok` are filled
    bool direct_slots = false;       /// the slot of a value is its offset from the minimum, not a hash
    UInt32 run_tok[MAX_RUN_EXP + 1] = {};
    std::vector<UInt16> id2tok;
    std::vector<UInt16> ans_bits;
    std::vector<UInt8> ans_nbits;
    std::vector<UInt32> tmp;
    std::vector<UInt32> touched;
    std::vector<UInt16> freq_scratch;
    std::vector<HeapItem> heap_scratch;
    AnsEncodeTable enc_tables[MAX_CONTEXTS];

    StreamEncoder()
    {
        fine.init(MAX_SPLIT, MAX_MSB);
    }

    static constexpr double HEADER_BITS_PER_SYMBOL = 12;
    static constexpr double DICT_HEADER_BITS = 26;
    static constexpr UInt32 STRIPE = 256;
    static constexpr UInt32 DICT_SAMPLE = 512;
    static constexpr UInt32 PROV_RUN = 1u << 29;
    static constexpr UInt16 NO_TOKEN = 0xffff;

    inline UInt32 binIdOf(Int64 v) const
    {
        if (model.fixed)
            return UInt32((UInt64(v) - UInt64(model.fmin)) >> model.shift);
        return (layout.classOf(magnitude(v)) << 1) | (v < 0);
    }

    /// The number of raw bits of the bin that holds the value.
    inline UInt32 valueBits(Int64 v) const
    {
        if (model.fixed)
            return model.shift;
        UInt64 mag = magnitude(v);
        if (mag < (UInt64(1) << layout.split))
            return 0;
        UInt32 e = 63 - std::countl_zero(mag);
        return e - layout.exp_msb[e];
    }

    /// Takes up to DICT_SAMPLE values that carry raw bits, sorted. Returns their number.
    UInt32 sampleForDictionary(Int64 * sample) const
    {
        const Int64 * r = data;
        UInt32 ns = 0;
        UInt32 step = std::max<UInt32>(1, n / DICT_SAMPLE);
        for (UInt32 i = 0; i < n && ns < DICT_SAMPLE; i += step)
        {
            Int64 v = r[i];
            if (v == 0 && model.runs)
                continue;
            if (valueBits(v) == 0)
                continue;
            sample[ns++] = v;
        }
        std::sort(sample, sample + ns);
        return ns;
    }

    /// Looks at a sample of the residuals: every `1 << skip_log`-th stripe. Chooses the family of bins and the
    /// zero handling (the estimate of the size that it leaves does not know about the dictionary).
    /// `try_fixed` allows the fixed-width bins.
    void analyze(const Int64 * r, UInt32 count, bool try_fixed, UInt32 skip_log)
    {
        const double * xlx = xlog2xTable();
        data = r;
        n = count;
        if (n < 4 * STRIPE)
            skip_log = 0;
        const UInt32 period = STRIPE << skip_log;

        /// --- Pass 1 over the sample: fine class histogram, zero runs, range.
        fine_hist.assign(size_t(fine.num_classes) * 2, 0);
        fine_used.clear();
        UInt32 run_hist[MAX_RUN_EXP + 1] = {};
        UInt32 zeros = 0;
        UInt32 zero_runs = 0;
        UInt32 sampled = 0;
        Int64 rmin = r[0];
        Int64 rmax = r[0];
        for (UInt32 start = 0; start < n; start += period)
        {
            UInt32 stop = std::min(n, start + STRIPE);
            sampled += stop - start;
            for (UInt32 i = start; i < stop;)
            {
                Int64 v = r[i];
                if (v == 0)
                {
                    UInt32 j = i + 1;
                    while (j < stop && r[j] == 0)
                        ++j;
                    UInt32 len = j - i;
                    zeros += len;
                    ++zero_runs;
                    ++run_hist[31 - std::countl_zero(len)];
                    i = j;
                    continue;
                }
                rmin = std::min(rmin, v);
                rmax = std::max(rmax, v);
                UInt32 id = (fine.classOf(magnitude(v)) << 1) | (v < 0);
                if (fine_hist[id]++ == 0)
                    fine_used.push_back(id);
                ++i;
            }
        }
        if (zeros)
        {
            rmin = std::min<Int64>(rmin, 0);
            rmax = std::max<Int64>(rmax, 0);
        }
        const UInt32 nonzero = sampled - zeros;
        const double scale = double(n) / sampled;
        double run_sum_xlx = 0;
        double run_raw = 0;
        UInt32 run_symbols = 0;
        for (UInt32 e = 0; e <= MAX_RUN_EXP; ++e)
        {
            if (run_hist[e])
            {
                run_sum_xlx += xlx[run_hist[e]];
                run_raw += double(e) * run_hist[e];
                ++run_symbols;
            }
        }

        struct Choice
        {
            double bits = 1e300;
            bool fixed = false;
            UInt32 split = 0;
            UInt32 msb = 0;
            UInt32 shift = 0;
            bool runs = false;
        };
        Choice best;
        /// `zero_bin_extra` is the number of non-zero values in the bin that the zeros would join when they are
        /// ordinary tokens (~0u when they get a bin of their own); `zero_raw` is the raw bits per such zero.
        auto consider = [&](double sum_xlx, double raw_bits, UInt32 used, UInt32 zero_bin_extra, double zero_raw, Choice c)
        {
            {
                double total = sampled;
                double s = sum_xlx;
                UInt32 symbols = used;
                if (zeros)
                {
                    if (zero_bin_extra != ~0u && zero_bin_extra != 0)
                        s += xlx[std::min<UInt32>(zeros + zero_bin_extra, CHUNK_MAX)] - xlx[zero_bin_extra];
                    else
                    {
                        s += xlx[zeros];
                        ++symbols;
                    }
                }
                double bits = (total * std::log2(total) - s + raw_bits + zero_raw * zeros) * scale + HEADER_BITS_PER_SYMBOL * symbols;
                if (bits < best.bits)
                {
                    best = c;
                    best.bits = bits;
                    best.runs = false;
                }
            }
            if (zeros)
            {
                double total = double(nonzero) + zero_runs;
                double bits = (total * std::log2(total) - sum_xlx - run_sum_xlx + raw_bits + run_raw) * scale
                    + HEADER_BITS_PER_SYMBOL * (used + run_symbols);
                if (bits < best.bits)
                {
                    best = c;
                    best.bits = bits;
                    best.runs = true;
                }
            }
        };

        /// --- Logarithmic classes: derive every (split, msb) histogram from the fine one.
        struct FineInfo
        {
            UInt64 mag_base;
            UInt32 count;
            UInt32 sign;
        };
        std::vector<FineInfo> infos(fine_used.size());
        for (size_t k = 0; k < fine_used.size(); ++k)
        {
            UInt32 id = fine_used[k];
            UInt64 mag_base;
            UInt32 nb_fine;
            fine.classRange(id >> 1, mag_base, nb_fine);
            infos[k] = {mag_base, fine_hist[id], id & 1};
        }
        {
            ClassLayout coarse;
            for (UInt32 split = 0; split <= MAX_SPLIT; ++split)
            {
                for (UInt32 msb = 0; msb <= std::min(split, MAX_MSB); ++msb)
                {
                    coarse.init(split, msb);
                    tmp.assign(size_t(coarse.num_classes) * 2, 0);
                    touched.clear();
                    double raw_bits = 0;
                    for (const FineInfo & fi : infos)
                    {
                        UInt32 cid = (coarse.classOf(fi.mag_base) << 1) | fi.sign;
                        if (tmp[cid] == 0)
                            touched.push_back(cid);
                        tmp[cid] += fi.count;
                        if (fi.mag_base >= (UInt64(1) << split))
                        {
                            UInt32 e = 63 - std::countl_zero(fi.mag_base);
                            raw_bits += double(e - coarse.exp_msb[e]) * fi.count;
                        }
                    }
                    double sum_xlx = 0;
                    for (UInt32 cid : touched)
                        sum_xlx += xlx[std::min<UInt32>(tmp[cid], CHUNK_MAX)];
                    Choice c;
                    c.split = split;
                    c.msb = msb;
                    consider(sum_xlx, raw_bits, UInt32(touched.size()), ~0u, 0, c);
                }
            }
        }

        /// --- Fixed-width bins from the minimum.
        if (try_fixed)
        {
            UInt64 range = UInt64(rmax) - UInt64(rmin);
            UInt32 s0 = bitLength(range) > 12 ? bitLength(range) - 12 : 0;
            UInt32 bins = UInt32(range >> s0) + 1;
            fixed_hist.assign(bins, 0);
            for (UInt32 start = 0; start < n; start += period)
            {
                UInt32 stop = std::min(n, start + STRIPE);
                for (UInt32 i = start; i < stop; ++i)
                    if (r[i] != 0)
                        ++fixed_hist[(UInt64(r[i]) - UInt64(rmin)) >> s0];
            }
            UInt64 zero_pos = UInt64(0) - UInt64(rmin);
            for (UInt32 s = s0; s <= std::min(s0 + 10, MAX_RAW_BITS); ++s)
            {
                double sum_xlx = 0;
                UInt32 used = 0;
                for (UInt32 b = 0; b < bins; ++b)
                {
                    if (fixed_hist[b])
                    {
                        sum_xlx += xlx[fixed_hist[b]];
                        ++used;
                    }
                }
                UInt32 zero_bin = UInt32(zero_pos >> s);
                UInt32 zero_bin_extra = zero_bin < bins ? fixed_hist[zero_bin] : 0;
                Choice c;
                c.fixed = true;
                c.shift = s;
                consider(sum_xlx, double(s) * nonzero, used, zero_bin_extra, double(s), c);
                if (bins <= 1)
                    break;
                UInt32 nb = (bins + 1) / 2;
                for (UInt32 b = 0; b < nb; ++b)
                    fixed_hist[b] = fixed_hist[2 * b] + (2 * b + 1 < bins ? fixed_hist[2 * b + 1] : 0);
                bins = nb;
            }
        }

        model = StreamModel();
        model.fixed = best.fixed;
        model.split = UInt8(best.split);
        model.msb = UInt8(best.msb);
        model.shift = UInt8(best.shift);
        model.fmin = rmin;
        model.runs = best.runs;
        if (!model.fixed)
            layout.init(model.split, model.msb);
        estimated_bits = best.bits;
    }

    /// Does all the work for the stream that was analyzed: tokenizes the residuals with the chosen family of
    /// bins and builds the model.
    void finalize()
    {
        count();
        complete();
    }

    /// The first stage: the bins of the values, the dictionary, the alphabet and the counts of its tokens.
    /// Leaves an estimate of the size in `counted_bits`, which is enough to compare candidates.
    void count()
    {
        const double * xlx = xlog2xTable();
        const Int64 * r = data;
        const bool runs = model.runs;

        /// --- The exact range for the fixed bins: the analysis has seen only a sample.
        Int64 rmin = r[0];
        Int64 rmax = r[0];
        bool have_range = false;
        auto findRange = [&]
        {
            for (UInt32 i = 1; i < n; ++i)
            {
                rmin = std::min(rmin, r[i]);
                rmax = std::max(rmax, r[i]);
            }
            have_range = true;
        };
        if (model.fixed)
        {
            findRange();
            model.fmin = rmin;
            UInt64 range = UInt64(rmax) - UInt64(rmin);
            while (model.shift < MAX_RAW_BITS && (range >> model.shift) >= MAX_FIXED_BINS)
                ++model.shift;
        }
        const UInt32 max_bin_id = model.fixed ? MAX_FIXED_BINS : layout.num_classes * 2;

        /// --- Pass A: the bin of every value, the counts of the bins, the zero runs.
        prov.resize(n);
        bin_counts.assign(max_bin_id, 0);
        UInt32 run_counts[MAX_RUN_EXP + 1] = {};
        UInt32 items = 0;
        for (UInt32 i = 0; i < n;)
        {
            Int64 v = r[i];
            if (v == 0 && runs)
            {
                UInt32 j = i + 1;
                while (j < n && r[j] == 0)
                    ++j;
                UInt32 e = 31 - std::countl_zero(j - i);
                ++run_counts[e];
                prov[items++] = PROV_RUN | i;   /// the position; the length is recomputed in pass B
                i = j;
                continue;
            }
            UInt32 id = binIdOf(v);
            ++bin_counts[id];
            prov[items++] = id;
            ++i;
        }

        /// --- Dictionary: exact values that are frequent enough to deserve own symbols.
        promoted.clear();
        dict_counted = false;
        {
            Int64 sample[DICT_SAMPLE];
            UInt32 ns = sampleForDictionary(sample);
            UInt32 dup = 0;
            for (UInt32 i = 1; i < ns; ++i)
                dup += sample[i] == sample[i - 1];
            if (ns >= 16 && dup * 50 >= ns)
            {
                /// Count all exact values, and remember the slot of every value: the tokens are then found
                /// without looking the values up again.
                slots.resize(n);
                UInt32 * const slot_of = slots.data();
                if (!have_range)
                    findRange();
                const UInt64 range = UInt64(rmax) - UInt64(rmin);
                direct_slots = range < 2 * UInt64(n);
                UInt32 cap;
                UInt32 num_slots = 0;
                if (direct_slots)
                {
                    /// A narrow range: the value, counted from the minimum, is its slot. No hashing, no collisions.
                    cap = UInt32(range) + 1;
                    if (hash_keys.size() < cap)
                        hash_keys.resize(cap);
                    hash_counts.assign(cap, 0);
                    hash_slots.resize(std::min(cap, n));
                    UInt64 * const keys = hash_keys.data();
                    UInt32 * const counts = hash_counts.data();
                    UInt32 * const used = hash_slots.data();
                    for (UInt32 i = 0; i < n; ++i)
                    {
                        Int64 v = r[i];
                        if (v == 0 && runs)
                            continue;
                        UInt32 h = UInt32(UInt64(v) - UInt64(rmin));
                        if (counts[h]++ == 0)
                        {
                            keys[h] = UInt64(v);
                            used[num_slots++] = h;
                        }
                        slot_of[i] = h;
                    }
                }
                else
                {
                    /// An open-addressing hash table. A dictionary cannot hold more than the alphabet, so a small
                    /// table is tried first: it stays in the cache. Unless the sample has so few repetitions
                    /// that the small table is going to overflow.
                    const bool many = ns == DICT_SAMPLE && dup * 12 < ns;
                    cap = many ? std::bit_ceil(n * 2) : std::min<UInt32>(std::bit_ceil(n * 2), 2 * MAX_ALPHABET);
                    while (true)
                    {
                        if (hash_keys.size() < cap)
                            hash_keys.resize(cap);
                        hash_counts.assign(cap, 0);
                        const UInt32 limit = cap / 2;
                        hash_slots.resize(limit + 1);
                        UInt64 * const keys = hash_keys.data();
                        UInt32 * const counts = hash_counts.data();
                        UInt32 * const used = hash_slots.data();
                        const UInt32 hmask = cap - 1;
                        const UInt32 hshift = 64 - std::countr_zero(cap);
                        num_slots = 0;
                        bool overflow = false;
                        for (UInt32 i = 0; i < n; ++i)
                        {
                            Int64 v = r[i];
                            if (v == 0 && runs)
                                continue;
                            UInt32 h = UInt32((UInt64(v) * 0x9E3779B97F4A7C15ull) >> hshift);
                            while (counts[h] && keys[h] != UInt64(v))
                                h = (h + 1) & hmask;
                            if (counts[h]++ == 0)
                            {
                                keys[h] = UInt64(v);
                                used[num_slots++] = h;
                                if (num_slots > limit)
                                {
                                    overflow = true;
                                    break;
                                }
                            }
                            slot_of[i] = h;
                        }
                        if (!overflow)
                            break;
                        cap = std::bit_ceil(n * 2);
                    }
                }
                hash_slots.resize(num_slots);
                dict_counted = true;
                slot_tok.assign(cap, NO_TOKEN);
                const double log_total = xlx[items] / items;
                for (UInt32 h : hash_slots)
                {
                    UInt32 c = hash_counts[h];
                    if (c < 2)
                        continue;
                    Int64 v = Int64(hash_keys[h]);
                    UInt32 nb = valueBits(v);
                    if (nb == 0)
                        continue;
                    UInt32 in_bin = bin_counts[binIdOf(v)];
                    double class_cost = log_total - xlx[in_bin] / in_bin + nb;
                    double exact_cost = log_total - xlx[c] / c;
                    if (c * (class_cost - exact_cost) > DICT_HEADER_BITS)
                        promoted.push_back({v, c});
                }
                /// Keep the alphabet within the limit: prefer the most frequent values.
                UInt32 used_bins = 0;
                for (UInt32 id = 0; id < max_bin_id; ++id)
                    used_bins += bin_counts[id] != 0;
                UInt32 max_dict = MAX_ALPHABET - std::min<UInt32>(used_bins + MAX_RUN_EXP + 1, MAX_ALPHABET);
                if (promoted.size() > max_dict)
                {
                    std::nth_element(promoted.begin(), promoted.begin() + max_dict, promoted.end(),
                        [](const auto & a, const auto & b) { return a.second > b.second; });
                    promoted.resize(max_dict);
                }
                std::sort(promoted.begin(), promoted.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
                const UInt32 hmask = cap - 1;
                const UInt32 hshift = direct_slots ? 0 : 64 - std::countr_zero(cap);
                for (size_t k = 0; k < promoted.size(); ++k)
                {
                    Int64 v = promoted[k].first;
                    bin_counts[binIdOf(v)] -= promoted[k].second;
                    UInt32 h;
                    if (direct_slots)
                        h = UInt32(UInt64(v) - UInt64(rmin));
                    else
                    {
                        h = UInt32((UInt64(v) * 0x9E3779B97F4A7C15ull) >> hshift);
                        while (hash_keys[h] != UInt64(v))
                            h = (h + 1) & hmask;
                    }
                    slot_tok[h] = UInt16(k);
                }
            }
        }

        /// --- Dense numbering of the symbols: dictionary, used bins, runs.
        model.dict.clear();
        for (auto & [v, c] : promoted)
            model.dict.push_back(v);
        const UInt32 nd = UInt32(model.dict.size());
        model.bin_ids.clear();
        id2tok.assign(max_bin_id, 0);
        for (UInt32 id = 0; id < max_bin_id; ++id)
        {
            if (bin_counts[id])
            {
                id2tok[id] = UInt16(nd + model.bin_ids.size());
                model.bin_ids.push_back(id);
            }
        }
        std::fill(run_tok, run_tok + MAX_RUN_EXP + 1, 0u);
        model.run_mask = 0;
        {
            UInt32 t = nd + UInt32(model.bin_ids.size());
            for (UInt32 e = 0; e <= MAX_RUN_EXP; ++e)
            {
                if (run_counts[e])
                {
                    model.run_mask |= UInt16(1u << e);
                    run_tok[e] = t++;
                }
            }
        }
        model.nctx = 1;
        if (!table.build(model))
            logicalError("encoder built an invalid model");
        const UInt32 alphabet = table.alphabet;

        /// --- The counts of the tokens are known without making the tokens.
        ntok = items;
        token_counts.assign(alphabet, 0);
        for (UInt32 k = 0; k < nd; ++k)
            token_counts[k] = promoted[k].second;
        for (size_t k = 0; k < model.bin_ids.size(); ++k)
            token_counts[nd + k] = bin_counts[model.bin_ids[k]];
        for (UInt32 e = 0; e <= MAX_RUN_EXP; ++e)
            if (run_counts[e])
                token_counts[run_tok[e]] = run_counts[e];
        counted_bits = estimateFromCounts();
    }

    /// What `complete` is going to find, short of the gain of the contexts: the size of the payload with the
    /// frequencies rounded to the table, and the size of the model.
    double estimateFromCounts() const
    {
        const double * lg = log2Table();
        const UInt32 alphabet = table.alphabet;
        const UInt32 min_log = std::max<UInt32>(MIN_TABLE_LOG, bitLength(alphabet - 1));
        const UInt32 lo_log = std::max(min_log, std::min<UInt32>(9, bitLength(ntok)));
        const UInt32 tl = std::max(lo_log, std::min<UInt32>(MAX_TABLE_LOG, bitLength(ntok) + 1));
        const UInt32 size = 1u << tl;
        const double scale = double(size) / ntok;
        UInt64 sum = 0;
        UInt64 raw_bits = 0;
        double bits = 0;
        UInt32 lengths[65] = {};
        for (UInt32 t = 0; t < alphabet; ++t)
        {
            UInt32 c = token_counts[t];
            UInt32 f = std::max<UInt32>(1, UInt32(c * scale + 0.5));
            sum += f;
            bits += c * (double(tl) - lg[f]);
            raw_bits += UInt64(c) * table.nbits[t];
            ++lengths[bitLength(f)];
        }
        /// The frequencies are then scaled to the sum of exactly the size of the table.
        bits += ntok * std::log2(double(sum) / size);
        bits += double(raw_bits);

        /// The model: the frequencies, the dictionary, the bins and the fixed fields.
        UInt64 length_prefixed = 0;
        for (UInt32 b = 0; b <= MAX_TABLE_LOG + 1; ++b)
            length_prefixed += UInt64(lengths[b]) * (4 + (b > 1 ? b - 1 : 0));
        UInt64 exp_golomb = 0;
        bestExpGolombOrder(lengths, 12, exp_golomb);
        bits += double(std::min(length_prefixed, exp_golomb));
        if (!model.dict.empty())
        {
            UInt32 gaps[65] = {};
            for (size_t i = 1; i < model.dict.size(); ++i)
                ++gaps[bitLength(UInt64(model.dict[i]) - UInt64(model.dict[i - 1]) - 1)];
            UInt64 gap_bits = 0;
            bestExpGolombOrder(gaps, 60, gap_bits);
            bits += double(gap_bits) + 2.0 * bitLength(magnitude(model.dict[0])) + 8;
        }
        bits += 4.0 * double(model.bin_ids.size()) + 64;
        return bits;
    }

    /// The second stage: the tokens, the contexts, the frequencies and the header of the model.
    void complete()
    {
        const double * xlx = xlog2xTable();
        const Int64 * r = data;
        const UInt32 alphabet = table.alphabet;

        /// --- Pass B: the tokens.
        tok.resize(ntok);
        raw.resize(ntok);
        rawbits.resize(ntok);
        const Int64 * lo = table.lo;
        const UInt8 * nbv = table.nbits;
        UInt32 pos = 0;
        for (UInt32 k = 0; k < ntok; ++k)
        {
            UInt32 p = prov[k];
            UInt32 t;
            if (p & PROV_RUN)
            {
                UInt32 i = p & ~PROV_RUN;
                UInt32 j = i + 1;
                while (j < n && r[j] == 0)
                    ++j;
                UInt32 len = j - i;
                UInt32 e = 31 - std::countl_zero(len);
                t = run_tok[e];
                raw[k] = len - (1u << e);
                rawbits[k] = UInt8(e);
                pos = j;
            }
            else
            {
                /// Both lookups are done and one is selected: the kind of a token is hard to predict.
                UInt32 dict_tok = dict_counted ? slot_tok[slots[pos]] : NO_TOKEN;
                UInt32 bin_tok = id2tok[p];
                t = dict_tok == NO_TOKEN ? bin_tok : dict_tok;
                Int64 v = r[pos++];
                raw[k] = UInt64(v) - UInt64(lo[t]);
                rawbits[k] = nbv[t];
            }
            tok[k] = UInt16(t);
        }

        /// --- Contexts: the magnitude class of the previous token.
        auto entropyOf = [&](const UInt32 * c, UInt32 total) -> double
        {
            if (!total)
                return 0;
            double s = 0;
            for (UInt32 t = 0; t < alphabet; ++t)
                s += xlx[c[t]];
            return xlx[total] - s;
        };
        auto usedOf = [&](const UInt32 * c)
        {
            UInt32 used = 0;
            for (UInt32 t = 0; t < alphabet; ++t)
                used += c[t] != 0;
            return used;
        };
        double best_ctx_bits = entropyOf(token_counts.data(), ntok) + 8.0 * usedOf(token_counts.data());
        UInt32 best_nctx = 1;
        UInt8 best_thr[MAX_CONTEXTS - 1] = {};
        std::vector<UInt32> & trial = tmp;
        if (ntok >= 256 && alphabet >= 2 && alphabet * 4 <= ntok)
        {
            UInt8 magbits[MAX_ALPHABET];
            UInt32 mb_hist[65] = {};
            for (UInt32 t = 0; t < alphabet; ++t)
            {
                if (t >= table.first_run)
                    magbits[t] = 0;
                else
                {
                    Int64 l = table.lo[t];
                    Int64 h = Int64(UInt64(l) + lowMask(table.nbits[t]));
                    magbits[t] = UInt8(std::max(bitLength(magnitude(l)), bitLength(magnitude(h))));
                }
                mb_hist[magbits[t]] += token_counts[t];
            }
            /// The trials: the tokens are split into 2 .. MAX_CONTEXTS groups of about equal weight by their
            /// magnitude. All the trials are counted in one pass over the tokens.
            struct Trial
            {
                UInt32 nctx = 0;
                UInt8 thr[MAX_CONTEXTS - 1] = {};
                UInt32 offset = 0;      /// of the counts in `trial`
            };
            Trial trials[MAX_CONTEXTS - 1];
            UInt32 num_trials = 0;
            /// The classes of a token in every trial, packed by 2 bits.
            UInt8 cls[MAX_ALPHABET];
            std::fill(cls, cls + alphabet, UInt8(0));
            UInt32 trial_size = 0;
            for (UInt32 nctx = 2; nctx <= MAX_CONTEXTS; ++nctx)
            {
                Trial & tr = trials[num_trials];
                tr = Trial();
                tr.nctx = nctx;
                UInt32 acc = 0;
                UInt32 q = 1;
                for (UInt32 b = 0; b < 65 && q < nctx; ++b)
                {
                    acc += mb_hist[b];
                    while (q < nctx && UInt64(acc) * nctx >= UInt64(ntok) * q)
                    {
                        tr.thr[q - 1] = UInt8(b);
                        ++q;
                    }
                }
                bool distinct = true;
                for (UInt32 q2 = 1; q2 + 1 < nctx; ++q2)
                    distinct &= tr.thr[q2] > tr.thr[q2 - 1];
                if (!distinct)
                    continue;
                for (UInt32 t = 0; t < alphabet; ++t)
                {
                    UInt8 c = 0;
                    for (UInt32 q2 = 0; q2 + 1 < nctx; ++q2)
                        c += magbits[t] > tr.thr[q2];
                    cls[t] |= UInt8(c << (2 * num_trials));
                }
                tr.offset = trial_size;
                trial_size += nctx * alphabet;
                ++num_trials;
            }
            static_assert(MAX_CONTEXTS == 3, "the classes of two trials are packed into a byte");
            trial.assign(trial_size, 0);
            /// The context of a token is the class of the previous token of the same ANS lane, so that
            /// the lanes stay independent in the decoder.
            if (num_trials == 2)
            {
                UInt32 * counts0 = trial.data() + trials[0].offset;
                UInt32 * counts1 = trial.data() + trials[1].offset;
                UInt32 prev[ANS_LANES] = {};
                for (UInt32 i = 0; i < ntok; ++i)
                {
                    UInt32 t = tok[i];
                    UInt32 lane = i & (ANS_LANES - 1);
                    UInt32 pc = prev[lane];
                    ++counts0[size_t(pc & 3) * alphabet + t];
                    ++counts1[size_t(pc >> 2) * alphabet + t];
                    prev[lane] = cls[t];
                }
            }
            else if (num_trials == 1)
            {
                UInt32 * counts0 = trial.data();
                UInt32 prev[ANS_LANES] = {};
                for (UInt32 i = 0; i < ntok; ++i)
                {
                    UInt32 t = tok[i];
                    UInt32 lane = i & (ANS_LANES - 1);
                    ++counts0[size_t(prev[lane]) * alphabet + t];
                    prev[lane] = cls[t];
                }
            }
            for (UInt32 k = 0; k < num_trials; ++k)
            {
                const Trial & tr = trials[k];
                const UInt32 * counts = trial.data() + tr.offset;
                double bits = 0;
                for (UInt32 c = 0; c < tr.nctx; ++c)
                {
                    UInt32 total = 0;
                    for (UInt32 t = 0; t < alphabet; ++t)
                        total += counts[size_t(c) * alphabet + t];
                    bits += entropyOf(counts + size_t(c) * alphabet, total)
                        + 8.0 * usedOf(counts + size_t(c) * alphabet) + 2.0 * alphabet;
                }
                /// Contexts slow the decoder down: require a real gain.
                if (bits < best_ctx_bits * 0.995)
                {
                    best_ctx_bits = bits;
                    best_nctx = tr.nctx;
                    copyBytes(best_thr, tr.thr, sizeof(tr.thr));
                    ctx_counts.assign(counts, counts + size_t(tr.nctx) * alphabet);
                }
            }
        }
        if (best_nctx == 1)
            ctx_counts.assign(token_counts.begin(), token_counts.end());
        model.nctx = UInt8(best_nctx);
        copyBytes(model.thresholds, best_thr, sizeof(best_thr));
        if (!table.build(model))
            logicalError("encoder built an invalid model");

        /// --- Table size and normalized frequencies.
        UInt32 min_log = std::max<UInt32>(MIN_TABLE_LOG, bitLength(alphabet - 1));
        /// Small tables lose precision, large ones take longer to build in the decoder; a short stream does not
        /// need a large table at all.
        UInt32 lo_log = std::max(min_log, std::min<UInt32>(9, bitLength(ntok)));
        UInt32 hi_log = std::max(lo_log, std::min<UInt32>(MAX_TABLE_LOG, bitLength(ntok) + 1));
        UInt32 best_log = lo_log;
        double best_log_bits = 1e300;
        freq_scratch.resize(size_t(model.nctx) * alphabet);
        for (UInt32 tl = lo_log; tl <= hi_log; ++tl)
        {
            double bits = 0;
            for (UInt32 c = 0; c < model.nctx; ++c)
            {
                const UInt32 * cc = ctx_counts.data() + size_t(c) * alphabet;
                UInt32 total = 0;
                for (UInt32 t = 0; t < alphabet; ++t)
                    total += cc[t];
                UInt16 * fc = freq_scratch.data() + size_t(c) * alphabet;
                if (!total)
                {
                    /// An unused context still needs a valid table.
                    std::fill(fc, fc + alphabet, 0);
                    fc[0] = UInt16(1u << tl);
                    continue;
                }
                normalizeCounts(cc, alphabet, tl, fc, heap_scratch);
                bits += normalizedCost(cc, fc, alphabet, tl) + double(frequencyHeaderBits(fc, alphabet));
            }
            bits += 0.03 * (1u << tl) * model.nctx;
            if (bits < best_log_bits)
            {
                best_log_bits = bits;
                best_log = tl;
                model.freqs.assign(freq_scratch.begin(), freq_scratch.end());
            }
        }
        model.table_log = UInt8(best_log);

        UInt64 raw_total = 0;
        for (UInt32 t = 0; t < alphabet; ++t)
            raw_total += UInt64(token_counts[t]) * table.nbits[t];
        estimated_bits = best_log_bits + double(raw_total);

        /// The header of the model, written once: its exact size takes part in the choice between
        /// candidates, and the bits are then copied into the header of the chunk.
        UInt8 * start = model_bytes.resize(size_t(MAX_ALPHABET) * 40 + 4096);
        BitWriter w(start);
        writeModel(w, model);
        model_bits = UInt64(w.p - start) * 8 + w.cnt;
        w.finish();
    }

    /// Writes the model into the header and returns the payload: [ANS region][raw region].
    /// The header gets: number of tokens - 1 (14 bits), sizes of the two regions in bytes (16 and 18 bits).
    void write(BitWriter & header, std::vector<UInt8> & payload)
    {
        UInt32 alphabet = table.alphabet;
        for (UInt32 c = 0; c < model.nctx; ++c)
            enc_tables[c].build(model.freqs.data() + size_t(c) * alphabet, alphabet, model.table_log);

        ans_bits.resize(ntok);
        ans_nbits.resize(ntok);
        const UInt32 size = 1u << model.table_log;
        UInt32 state[ANS_LANES] = {size, size, size, size};
        const UInt8 * cls = table.ctx;
        if (model.nctx == 1)
        {
            const AnsEncodeTable & t = enc_tables[0];
            for (UInt32 i = ntok; i-- > 0;)
                t.encode(state[i & (ANS_LANES - 1)], tok[i], ans_bits[i], ans_nbits[i]);
        }
        else
        {
            for (UInt32 i = ntok; i-- > 0;)
            {
                UInt32 c = i >= ANS_LANES ? cls[tok[i - ANS_LANES]] : 0;
                enc_tables[c].encode(state[i & (ANS_LANES - 1)], tok[i], ans_bits[i], ans_nbits[i]);
            }
        }

        payload.resize(size_t(ntok) * 9 + 64);
        BitWriter w(payload.data());
        for (UInt32 lane = 0; lane < ANS_LANES; ++lane)
            w.put(state[lane] - size, model.table_log);
        for (UInt32 i = 0; i < ntok; ++i)
            w.put(ans_bits[i], ans_nbits[i]);
        UInt8 * ans_end = w.finish();
        BitWriter wr(ans_end);
        for (UInt32 i = 0; i < ntok; ++i)
            wr.put(raw[i], rawbits[i]);
        UInt8 * raw_end = wr.finish();
        UInt32 ans_bytes = UInt32(ans_end - payload.data());
        UInt32 raw_bytes = UInt32(raw_end - ans_end);
        payload.resize(size_t(ans_bytes) + raw_bytes);

        const UInt8 * bytes = model_bytes.data();
        UInt64 left = model_bits;
        for (; left >= 56; left -= 56, bytes += 7)
            header.put(loadLE64(bytes) & lowMask(56), 56);
        if (left)
            header.put(loadLE64(bytes) & lowMask(UInt32(left)), UInt32(left));
        header.put(ntok - 1, 14);
        header.put(ans_bytes, 16);
        header.put(raw_bytes, 18);
    }
};

constexpr UInt8 FORMAT_VERSION = 2;
constexpr UInt8 BLOCK_FLAG_WIDE = 0x10;   /// 8-byte floats
constexpr UInt8 BLOCK_FLAG_RAW = 0x20;    /// the whole block is stored verbatim

enum ChunkMode : UInt8
{
    MODE_RAW = 0,
    MODE_CONST = 1,
    MODE_ENTROPY = 2,
    MODE_PACKED = 3,
};

enum FrontEnd : UInt8
{
    FRONT_BITS = 0,
    FRONT_DIV = 1,
    FRONT_MUL = 2,
};

enum ScaleKind : UInt8
{
    SCALE_RAW = 0,          /// the scale is stored as a double
    SCALE_POW10 = 1,        /// DIV: 10^e; MUL: 1 / 10^e
    SCALE_POW10_F32 = 2,    /// MUL only: 1 / 10^e rounded to Float32
};

enum ExceptionKind : UInt8
{
    EXC_NONE = 0,
    EXC_RAW = 1,            /// positions (UInt16) and values verbatim
    EXC_CODED = 2,          /// gaps between positions and values are entropy-coded
};

constexpr UInt32 MAX_ORDER = 7;

const double POW10[] = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
    1e19, 1e20, 1e21, 1e22};
constexpr UInt32 MAX_POW10 = 22;

template <typename F>
struct FloatTraits;

template <>
struct FloatTraits<Float32>
{
    using U = UInt32;
    static constexpr UInt32 W = 32;
    static constexpr UInt32 MAX_DECIMAL = 10;
    /// A decimal with more digits than this is not a short decimal for the type.
    static constexpr UInt32 SUSPICIOUS_DECIMAL = 6;
    static constexpr double MAX_DENOMINATOR = 1e7;
    static constexpr UInt32 MANTISSA_BITS = 24;
    /// Above this, consecutive integers divided by the denominator are closer than the neighboring floats.
    static constexpr double MAX_INTEGER = 16777216.0;
};

template <>
struct FloatTraits<Float64>
{
    using U = UInt64;
    static constexpr UInt32 W = 64;
    static constexpr UInt32 MAX_DECIMAL = 18;
    static constexpr UInt32 SUSPICIOUS_DECIMAL = 14;
    static constexpr double MAX_DENOMINATOR = 1e15;
    static constexpr UInt32 MANTISSA_BITS = 53;
    static constexpr double MAX_INTEGER = 9007199254740992.0;
};

template <typename F>
typename FloatTraits<F>::U floatBits(F v)
{
    return std::bit_cast<typename FloatTraits<F>::U>(v);
}

/// The values of a column are little-endian in the uncompressed data regardless of the host.
template <typename F>
typename FloatTraits<F>::U loadBits(const UInt8 * p)
{
    return unalignedLoadLittleEndian<typename FloatTraits<F>::U>(p);
}

template <typename F>
F loadFloat(const UInt8 * p)
{
    return std::bit_cast<F>(loadBits<F>(p));
}

template <typename F>
void storeBits(UInt8 * p, typename FloatTraits<F>::U bits)
{
    unalignedStoreLittleEndian<typename FloatTraits<F>::U>(p, bits);
}

template <typename F>
void storeFloat(UInt8 * p, F v)
{
    storeBits<F>(p, floatBits(v));
}

/// Order-preserving map of the bit pattern to an unsigned integer.
template <typename F>
typename FloatTraits<F>::U orderedBits(typename FloatTraits<F>::U b)
{
    using U = typename FloatTraits<F>::U;
    constexpr U sign = U(1) << (FloatTraits<F>::W - 1);
    return (b & sign) ? U(~b) : U(b | sign);
}

template <typename F>
typename FloatTraits<F>::U unorderedBits(typename FloatTraits<F>::U o)
{
    using U = typename FloatTraits<F>::U;
    constexpr U sign = U(1) << (FloatTraits<F>::W - 1);
    return (o & sign) ? U(o ^ sign) : U(~o);
}

/// `single` (Float32 only): the operation is done in single precision. It is faster, and it is how the values
/// produced by Float32 arithmetic are reproduced exactly.
template <typename F>
F reconstruct(Int64 q, bool mul, bool single, double scale)
{
    if (single)
        return mul ? F(float(q) * float(scale)) : F(float(q) / float(scale));
    return mul ? F(double(q) * scale) : F(double(q) / scale);
}

/// The integer candidate for the value: q = round(v * den), |q| < 2^53. False for NaN, infinities and huge values.
template <typename F>
bool integerCandidate(F v, double den, Int64 & q)
{
    double scaled = double(v) * den;
    if (!(std::fabs(scaled) < 9007199254740992.0))
        return false;
    q = std::llround(scaled);
    return true;
}

/// v == F(double(q) / den), bit-exact.
template <typename F>
bool toRational(F v, double den, Int64 & q)
{
    if (!integerCandidate(v, den, q))
        return false;
    return floatBits(F(double(q) / den)) == floatBits(v);
}

/// The minimal denominator q <= max_den with F(p / q) == |v| for an integer p, by continued fractions. 0 if none.
template <typename F>
double minimalDenominator(F v, double max_den)
{
    double x = std::fabs(double(v));
    if (!(x < 1e15))
        return 0;
    F target = F(x);
    double h0 = 0, h1 = 1, k0 = 1, k1 = 0;
    double r = x;
    for (int iter = 0; iter < 48; ++iter)
    {
        double a = std::floor(r);
        double h2 = a * h1 + h0;
        double k2 = a * k1 + k0;
        if (k2 > max_den || h2 > 9e15)
            return 0;
        h0 = h1;
        h1 = h2;
        k0 = k1;
        k1 = k2;
        if (floatBits(F(h1 / k1)) == floatBits(target))
            return k1;
        double frac = r - a;
        if (frac < 1e-300)
            return 0;
        r = 1.0 / frac;
    }
    return 0;
}

struct ChunkParams
{
    UInt8 front = FRONT_BITS;
    UInt8 shift = 0;            /// BITS: common trailing zero bits
    double scale = 1;        /// DIV: denominator; MUL: multiplier
    bool single = false;     /// DIV, MUL of Float32: single-precision operation
    UInt64 gcd = 1;
    Int64 base = 0;
    UInt8 order = 0;
    Int64 center = 0;
    Int64 initial[MAX_ORDER] = {};
    bool has_adj = false;
    UInt8 exc_kind = EXC_NONE;
    UInt32 exceptions = 0;
    Int64 exc_base = 0;        /// EXC_CODED: subtracted from the ordered bits of the exception values
};

void writeChunkParams(BitWriter & w, const ChunkParams & p)
{
    w.put(p.front, 2);
    if (p.front == FRONT_BITS)
        w.put(p.shift, 6);
    else
    {
        w.put(p.single, 1);
        UInt32 kind = SCALE_RAW;
        UInt32 e = 0;
        for (UInt32 i = 0; i <= MAX_POW10; ++i)
        {
            if (p.front == FRONT_DIV && p.scale == POW10[i])
            {
                kind = SCALE_POW10;
                e = i;
            }
            if (p.front == FRONT_MUL && p.scale == 1.0 / POW10[i])
            {
                kind = SCALE_POW10;
                e = i;
            }
            else if (p.front == FRONT_MUL && p.scale == double(Float32(1.0 / POW10[i])))
            {
                kind = SCALE_POW10_F32;
                e = i;
            }
        }
        w.put(kind, 2);
        if (kind == SCALE_RAW)
        {
            w.put64(std::bit_cast<UInt64>(p.scale), 64);
        }
        else
            w.put(e, 5);
    }
    w.put(p.gcd > 1, 1);
    if (p.gcd > 1)
        w.putSigned(Int64(p.gcd));
    w.putSigned(p.base);
    w.put(p.order, 3);
    if (p.order)
    {
        w.putSigned(p.center);
        for (UInt32 j = 0; j < p.order; ++j)
            w.putSigned(p.initial[j]);
    }
    w.put(p.has_adj, 1);
    w.put(p.exc_kind, 2);
    if (p.exc_kind != EXC_NONE)
        w.put(p.exceptions, 15);
    if (p.exc_kind == EXC_CODED)
        w.putSigned(p.exc_base);
}

void readChunkParams(BitReader & r, ChunkParams & p, UInt32 float_bits)
{
    p.front = UInt8(r.get(2));
    if (p.front == FRONT_BITS)
    {
        p.shift = UInt8(r.get(6));
        if (p.shift >= float_bits)
            corrupt("bad shift");
    }
    else if (p.front == FRONT_DIV || p.front == FRONT_MUL)
    {
        p.single = r.get(1);
        if (p.single && float_bits != 32)
            corrupt("single-precision reconstruction of a wide type");
        UInt32 kind = UInt32(r.get(2));
        if (kind == SCALE_RAW)
        {
            p.scale = std::bit_cast<double>(r.get64(64));
            if (!(p.scale > 0) || !std::isfinite(p.scale))
                corrupt("bad scale");
        }
        else
        {
            UInt32 e = UInt32(r.get(5));
            if (e > MAX_POW10 || kind > SCALE_POW10_F32 || (kind == SCALE_POW10_F32 && p.front != FRONT_MUL))
                corrupt("bad decimal scale");
            if (p.front == FRONT_DIV)
                p.scale = POW10[e];
            else if (kind == SCALE_POW10)
                p.scale = 1.0 / POW10[e];
            else
                p.scale = double(Float32(1.0 / POW10[e]));
        }
    }
    else
        corrupt("unknown front-end");
    p.gcd = 1;
    if (r.get(1))
    {
        p.gcd = UInt64(r.getSigned());
        if (p.gcd < 2)
            corrupt("bad multiplier");
    }
    p.base = r.getSigned();
    p.order = UInt8(r.get(3));
    p.center = 0;
    if (p.order)
    {
        p.center = r.getSigned();
        for (UInt32 j = 0; j < p.order; ++j)
            p.initial[j] = r.getSigned();
    }
    p.has_adj = r.get(1);
    p.exc_kind = UInt8(r.get(2));
    p.exceptions = 0;
    if (p.exc_kind > EXC_CODED)
        corrupt("bad exception kind");
    if (p.exc_kind != EXC_NONE)
        p.exceptions = UInt32(r.get(15));
    if (p.exc_kind == EXC_CODED)
        p.exc_base = r.getSigned();
    if (p.front == FRONT_BITS && (p.has_adj || p.exc_kind != EXC_NONE))
        corrupt("bad chunk parameters");
}

template <typename F>
struct Decoder
{
    using U = typename FloatTraits<F>::U;
    static constexpr UInt32 W = FloatTraits<F>::W;

    StreamDecoder stream;
    StreamDecoder adj_stream;
    StreamDecoder gap_stream;
    StreamDecoder exc_stream;
    Scratch<Int64> values;
    std::vector<Int64> adj;
    std::vector<Int64> gaps;
    std::vector<Int64> exc_values;
    std::vector<UInt32> positions;

    /// Decodes one chunk of n values; returns the number of consumed bytes.
    /// The source must have 8 readable bytes after `src + size`.
    size_t decodeChunk(const UInt8 * src, size_t size, UInt32 n, UInt8 * dst)
    {
        if (size < 1)
            corrupt("truncated chunk");
        UInt8 mode = src[0] & 3;
        if (mode == MODE_RAW)
        {
            size_t need = 1 + size_t(n) * sizeof(F);
            if (size < need)
                corrupt("truncated raw chunk");
            copyBytes(dst, src + 1, size_t(n) * sizeof(F));
            return need;
        }
        if (mode == MODE_CONST)
        {
            if (size < 1 + sizeof(F))
                corrupt("truncated constant chunk");
            for (UInt32 i = 0; i < n; ++i)
                copyBytes(dst + size_t(i) * sizeof(F), src + 1, sizeof(F));
            return 1 + sizeof(F);
        }
        if (mode != MODE_ENTROPY)
            corrupt("unknown chunk mode");

        BitReader r(src, size);
        r.get(2);
        ChunkParams p;
        readChunkParams(r, p, W);
        if (p.exceptions >= n)
            corrupt("bad number of exceptions");
        const UInt32 nreg = n - p.exceptions;
        if (p.order >= nreg)
            corrupt("bad predictor order");
        stream.parse(r, p.order ? p.center : 0);
        if (p.has_adj)
            adj_stream.parse(r, 0);
        if (p.exc_kind == EXC_CODED)
        {
            gap_stream.parse(r, 0);
            exc_stream.parse(r, p.exc_base);
        }
        size_t pos = r.bytesConsumed();
        size_t raw_exc_bytes = p.exc_kind == EXC_RAW ? size_t(p.exceptions) * (2 + sizeof(F)) : 0;
        size_t total = pos + raw_exc_bytes + stream.payloadSize();
        if (p.has_adj)
            total += adj_stream.payloadSize();
        if (p.exc_kind == EXC_CODED)
            total += gap_stream.payloadSize() + exc_stream.payloadSize();
        if (total > size)
            corrupt("truncated chunk");
        const UInt8 * raw_exc = src + pos;
        const UInt8 * payload = raw_exc + raw_exc_bytes;

        Int64 * v = values.resize(n);
        const UInt32 order = p.order;
        const UInt32 nexc = p.exceptions;
        const UInt8 * adj_payload = payload + stream.payloadSize();
        const UInt8 * exc_payload = adj_payload + (p.has_adj ? adj_stream.payloadSize() : 0);

        /// Exceptions first: their positions tell where the regular values go.
        positions.resize(nexc + 1);
        if (p.exc_kind == EXC_RAW)
        {
            UInt32 prev = 0;
            for (UInt32 k = 0; k < nexc; ++k)
            {
                UInt16 at = unalignedLoadLittleEndian<UInt16>(raw_exc + size_t(k) * 2);
                if (at >= n || (k && at <= prev))
                    corrupt("bad exception position");
                positions[k] = at;
                prev = at;
                copyBytes(dst + size_t(at) * sizeof(F), raw_exc + size_t(nexc) * 2 + size_t(k) * sizeof(F), sizeof(F));
            }
        }
        else if (p.exc_kind == EXC_CODED)
        {
            gaps.resize(nexc);
            exc_values.resize(nexc);
            gap_stream.decode(exc_payload, nexc, gaps.data());
            exc_stream.decode(exc_payload + gap_stream.payloadSize(), nexc, exc_values.data());
            UInt64 at = 0;
            for (UInt32 k = 0; k < nexc; ++k)
            {
                UInt64 gap = UInt64(gaps[k]);
                if (gap >= n)
                    corrupt("bad exception position");
                at += gap;
                if (at >= n)
                    corrupt("bad exception position");
                positions[k] = UInt32(at);
                U b = unorderedBits<F>(U(UInt64(exc_values[k])));
                storeBits<F>(dst + size_t(at) * sizeof(F), b);
                ++at;
            }
        }
        positions[nexc] = n;

        /// The base of the latent is folded into the tables when there is no multiplier.
        const UInt64 g = p.gcd;
        const UInt64 base = UInt64(p.base);
        const bool fold = g == 1;
        stream.setExtra(fold && order == 0 ? p.base : 0);

        /// The state of the inverse predictor: d[j] is the current difference of order j.
        UInt64 d[MAX_ORDER] = {};
        for (UInt32 i = 0; i < order; ++i)
        {
            d[i] = UInt64(p.initial[i]) + (fold && i == 0 ? base : 0);
            for (UInt32 j = i; j-- > 0;)
                d[j] += d[j + 1];
            v[i] = Int64(d[0]);
        }
        auto predict = [&](UInt32 from, UInt32 to)
        {
            if (order == 1)
            {
                UInt64 acc = d[0];
                for (UInt32 i = from; i < to; ++i)
                {
                    acc += UInt64(v[i]);
                    v[i] = Int64(acc);
                }
                d[0] = acc;
            }
            else if (order > 1)
            {
                for (UInt32 i = from; i < to; ++i)
                {
                    d[order - 1] += UInt64(v[i]);
                    for (UInt32 j = order - 1; j-- > 0;)
                        d[j] += d[j + 1];
                    v[i] = Int64(d[0]);
                }
            }
        };

        /// Latent -> float for a range of regular values; `out` is where the value number 0 would go.
        const double scale = p.scale;
        const bool mul = p.front == FRONT_MUL;
        auto convert = [&](UInt32 from, UInt32 to, UInt8 * out)
        {
            if (!fold)
            {
                for (UInt32 i = from; i < to; ++i)
                    v[i] = Int64(base + g * UInt64(v[i]));
            }
            if (p.front == FRONT_BITS)
            {
                const UInt32 shift = p.shift;
                const U low = U(lowMask(shift));
                constexpr U sign = U(1) << (W - 1);
                for (UInt32 i = from; i < to; ++i)
                {
                    U o = U(UInt64(v[i]) << shift);
                    U b = (o & sign) ? U(o ^ sign) : U(~o & ~low);
                    storeBits<F>(out + size_t(i) * sizeof(F), b);
                }
            }
            else if (p.single)
            {
                const float fs = float(scale);
                bool narrow = true;
                for (UInt32 i = from; i < to; ++i)
                    narrow &= v[i] == Int64(Int32(v[i]));
                if (narrow && mul)
                {
                    for (UInt32 i = from; i < to; ++i)
                    {
                        F f = F(float(Int32(v[i])) * fs);
                        storeFloat<F>(out + size_t(i) * sizeof(F), f);
                    }
                }
                else if (narrow)
                {
                    for (UInt32 i = from; i < to; ++i)
                    {
                        F f = F(float(Int32(v[i])) / fs);
                        storeFloat<F>(out + size_t(i) * sizeof(F), f);
                    }
                }
                else
                {
                    for (UInt32 i = from; i < to; ++i)
                    {
                        F f = reconstruct<F>(v[i], mul, true, scale);
                        storeFloat<F>(out + size_t(i) * sizeof(F), f);
                    }
                }
            }
            else if (mul)
            {
                for (UInt32 i = from; i < to; ++i)
                {
                    F f = F(double(v[i]) * scale);
                    storeFloat<F>(out + size_t(i) * sizeof(F), f);
                }
            }
            else
            {
                for (UInt32 i = from; i < to; ++i)
                {
                    F f = F(double(v[i]) / scale);
                    storeFloat<F>(out + size_t(i) * sizeof(F), f);
                }
            }
        };

        /// The regular value number i goes to the position i plus the number of exceptions before it.
        UInt32 passed = 0;
        auto convertAround = [&](UInt32 from, UInt32 to)
        {
            while (from < to)
            {
                UInt32 limit = positions[passed] - passed;
                if (passed < nexc && from >= limit)
                {
                    ++passed;
                    continue;
                }
                UInt32 stop = passed < nexc ? std::min(to, limit) : to;
                convert(from, stop, dst + size_t(passed) * sizeof(F));
                from = stop;
            }
        };

        if (stream.hasRuns())
        {
            stream.decodeRuns(payload, nreg - order, v + order);
            predict(order, nreg);
            convertAround(0, nreg);
        }
        else
        {
            /// In batches that stay in the L1 cache through all the stages.
            constexpr UInt32 BATCH = 2048;
            StreamDecoder::Cursor cursor;
            stream.begin(payload, cursor);
            UInt32 converted = 0;
            for (UInt32 done = order; done < nreg;)
            {
                UInt32 m = std::min(BATCH, nreg - done);
                stream.next(cursor, v + done, m);
                predict(done, done + m);
                done += m;
                convertAround(converted, done);
                converted = done;
            }
            stream.finish(cursor);
        }

        if (p.has_adj)
        {
            /// The adjustments are sparse: most values are reproduced exactly.
            UInt32 before = 0;
            adj_stream.decodeSparse(adj_payload, nreg, [&](UInt32 i, Int64 a)
            {
                while (before < nexc && positions[before] - before <= i)
                    ++before;
                UInt8 * at = dst + (size_t(i) + before) * sizeof(F);
                U b = loadBits<F>(at);
                b = unorderedBits<F>(U(orderedBits<F>(b) + U(UInt64(a))));
                storeBits<F>(at, b);
            });
        }
        return total;
    }

    /// The source must have 8 readable bytes after its end.
    void decompress(const UInt8 * src, size_t size, UInt8 * dst, size_t uncompressed_size)
    {
        if (size < 1)
            corrupt("empty block");
        UInt8 head = src[0];
        if ((head & 0x0f) != FORMAT_VERSION)
            corrupt("unknown format version");
        if (((head & BLOCK_FLAG_WIDE) != 0) != (sizeof(F) == 8))
            corrupt("float width mismatch");
        if (head & BLOCK_FLAG_RAW)
        {
            if (size != uncompressed_size + 1)
                corrupt("bad raw block size");
            copyBytes(dst, src + 1, uncompressed_size);
            return;
        }
        size_t n = uncompressed_size / sizeof(F);
        size_t tail = uncompressed_size % sizeof(F);
        size_t pos = 1;
        for (size_t off = 0; off < n; off += CHUNK_MAX)
        {
            UInt32 cn = UInt32(std::min<size_t>(CHUNK_MAX, n - off));
            pos += decodeChunk(src + pos, size - pos, cn, dst + off * sizeof(F));
        }
        if (size - pos != tail)
            corrupt("bad block size");
        copyBytes(dst + n * sizeof(F), src + pos, tail);
    }
};

/// Multipliers of a power of ten that occur as denominators of averaged or converted decimals.
const UInt32 DENOMINATOR_MULTIPLIERS[] = {2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 18, 24, 25, 30, 32, 36, 45, 48,
    60, 64, 72, 75, 90, 96, 120, 125, 128, 144, 180, 240, 256, 360, 512, 720, 1024};

template <typename F>
struct Encoder
{
    using U = typename FloatTraits<F>::U;
    static constexpr UInt32 W = FloatTraits<F>::W;
    static constexpr UInt32 SAMPLE = 64;

    /// The chunk converted with the DIV or MUL front-end.
    struct Converted
    {
        std::vector<Int64> lat;        /// integers of the regular values
        std::vector<Int64> adj;        /// their adjustments
        std::vector<UInt16> exc_pos;    /// positions of the exceptions
        std::vector<UInt16> inexact;    /// positions of the regular values with a non-zero adjustment
        bool mul = false;
        bool single = false;
        double scale = 1;
        double den = 1;
    };

    std::vector<Int64> lat_bits;
    Converted conv;
    Converted conv_saved;
    std::vector<Int64> diff[2];
    static constexpr UInt32 MAX_CANDIDATES = 3;
    static constexpr double PROXY_BAND = 1.35;        /// between the proxies of the candidates that are looked at
    static constexpr double VERY_CLOSE_CALL = 1.003;  /// between the estimates from the counts of the tokens
    std::vector<Int64> resid[MAX_CANDIDATES];
    std::vector<Int64> gaps;
    std::vector<Int64> exc_values;
    std::vector<UInt8> payload_main;
    std::vector<UInt8> payload_adj;
    std::vector<UInt8> payload_gap;
    std::vector<UInt8> payload_exc;
    std::vector<UInt8> scratch;
    std::vector<UInt64> bitmap;
    StreamEncoder streams[MAX_CANDIDATES];
    StreamEncoder adj_stream;
    StreamEncoder gap_stream;
    StreamEncoder exc_stream;

    struct Plan
    {
        double proxy = 1e300;
        double class_proxy = 1e300;  /// order 0: the size without a dictionary
        UInt8 order = 0;
        Int64 center = 0;
    };

    struct Candidate
    {
        bool div = false;
        Plan plan;
        double cost = 1e300;   /// the proxy plus the side costs
    };

    /// Estimate of the entropy-coded size in bits: classes by the bit length with zero as its own class.
    static double proxyCost(const UInt32 * hist, UInt32 total)
    {
        const double * xlx = xlog2xTable();
        double s = 0;
        double raw = 0;
        for (UInt32 b = 0; b < PROXY_CLASSES; ++b)
        {
            if (hist[b])
            {
                s += xlx[std::min<UInt32>(hist[b], CHUNK_MAX)];
                /// Classes 1 and 2 are +-1; then two classes (the signs) per bit length.
                if (b >= 3)
                    raw += double((b - 1) / 2) * hist[b];
            }
        }
        return xlx[total] - s + raw;
    }

    static constexpr UInt32 PROXY_CLASSES = 130;

    static inline UInt32 proxyClass(Int64 v)
    {
        if (v == 0)
            return 0;
        return 2 * bitLength(magnitude(v)) - 1 + (v < 0);
    }

    static double proxyOf(const Int64 * v, UInt32 n)
    {
        UInt32 hist[PROXY_CLASSES] = {};
        for (UInt32 i = 0; i < n; ++i)
            ++hist[proxyClass(v[i])];
        return proxyCost(hist, n);
    }

    /// The exact size in bits of the header of a completed stream: the model and the sizes.
    static double modelBits(const StreamEncoder & s)
    {
        return double(s.model_bits) + 48;
    }

    /// Evaluates the differencing orders for the latent `m` (wrapping arithmetic) and appends the promising plans.
    /// `distinct` is the estimated number of distinct values, for the dictionary variant of order 0.
    void choosePredictor(const Int64 * m, UInt32 n, double distinct, std::vector<Plan> & plans)
    {
        /// The orders are compared on a sample: a few stripes of consecutive values.
        constexpr UInt32 STRIPES = 8;
        constexpr UInt32 LENGTH = 256;
        Int64 buf[STRIPES * LENGTH];
        Int64 sorted[STRIPES * LENGTH];
        UInt32 nstripes;
        UInt32 length;
        if (n <= STRIPES * LENGTH)
        {
            nstripes = 1;
            length = n;
            std::copy(m, m + n, buf);
        }
        else
        {
            nstripes = STRIPES;
            length = LENGTH;
            for (UInt32 s = 0; s < STRIPES; ++s)
            {
                UInt32 start = UInt32(UInt64(s) * (n - LENGTH) / (STRIPES - 1));
                std::copy(m + start, m + start + LENGTH, buf + s * LENGTH);
            }
        }
        const UInt32 sampled = nstripes * length;

        UInt32 hist[PROXY_CLASSES];
        Plan zero;
        {
            std::fill(hist, hist + PROXY_CLASSES, 0);
            Int64 mn = buf[0];
            Int64 mx = buf[0];
            for (UInt32 i = 0; i < sampled; ++i)
            {
                ++hist[proxyClass(buf[i])];
                mn = std::min(mn, buf[i]);
                mx = std::max(mx, buf[i]);
            }
            zero.proxy = proxyCost(hist, sampled) * (double(n) / sampled);
            zero.class_proxy = zero.proxy;
            zero.order = 0;
            /// With few distinct values every one of them can be a dictionary symbol.
            if (distinct * 2 < n)
            {
                double range = double(UInt64(mx) - UInt64(mn));
                double entry = std::log2(range / distinct + 1) + 8;
                double dict = double(n) * std::log2(std::max(distinct, 1.0)) + distinct * entry;
                zero.proxy = std::min(zero.proxy, dict);
            }
        }
        Plan best;
        for (UInt32 order = 1; order <= MAX_ORDER && order < length; ++order)
        {
            /// In-place differencing of every stripe: after `order` rounds, the elements from `order` on hold
            /// the differences of that order.
            UInt32 ns = 0;
            for (UInt32 s = 0; s < nstripes; ++s)
            {
                Int64 * x = buf + s * length;
                for (UInt32 i = length - 1; i >= order; --i)
                    x[i] = Int64(UInt64(x[i]) - UInt64(x[i - 1]));
                for (UInt32 i = order; i < length; ++i)
                    sorted[ns++] = x[i];
            }
            /// Candidate center: the median of the differences.
            std::nth_element(sorted, sorted + ns / 2, sorted + ns);
            Int64 med = sorted[ns / 2];
            UInt32 hist_c[PROXY_CLASSES];
            std::fill(hist, hist + PROXY_CLASSES, 0);
            std::fill(hist_c, hist_c + PROXY_CLASSES, 0);
            for (UInt32 s = 0; s < nstripes; ++s)
            {
                const Int64 * x = buf + s * length;
                for (UInt32 i = order; i < length; ++i)
                {
                    ++hist[proxyClass(x[i])];
                    ++hist_c[proxyClass(Int64(UInt64(x[i]) - UInt64(med)))];
                }
            }
            double scale = double(n - order) / ns;
            double p0 = proxyCost(hist, ns) * scale + 64.0 * order;
            double pc = med ? proxyCost(hist_c, ns) * scale + 64.0 * order + 64 : 1e300;
            double p = std::min(p0, pc);
            bool improved = p < best.proxy;
            if (improved)
            {
                best.proxy = p;
                best.order = UInt8(order);
                best.center = pc < p0 ? med : 0;
            }
            /// Higher orders are tried only while they keep helping, and not at all when the first one is far
            /// behind no differencing.
            if ((order >= 2 && !improved) || (order == 1 && p > zero.class_proxy * 1.25))
                break;
        }
        plans.push_back(zero);
        if (best.order)
            plans.push_back(best);
    }

    /// Finds a denominator `den` such that most of the sampled values are F(integer / den). Returns 0 if none.
    static double detectDenominator(const F * sample, UInt32 ns)
    {
        UInt32 scale[SAMPLE];
        double max_abs = 0;
        for (UInt32 i = 0; i < ns; ++i)
        {
            scale[i] = 255;
            max_abs = std::max(max_abs, std::fabs(double(sample[i])));
            for (UInt32 e = 0; e <= FloatTraits<F>::MAX_DECIMAL; ++e)
            {
                Int64 q;
                if (toRational(sample[i], POW10[e], q))
                {
                    scale[i] = e;
                    break;
                }
            }
        }
        double best_cost = 1e300;
        double best_den = 0;
        UInt32 best_failures = ns;
        auto considerDen = [&](double den, UInt32 failures)
        {
            if (max_abs * den > FloatTraits<F>::MAX_INTEGER)
                return;
            double width = std::log2(max_abs * den + 1) + 1;
            double x = double(failures) / ns;
            double cost = (1 - x) * width + x * (16.0 + W);
            if (cost < best_cost)
            {
                best_cost = cost;
                best_den = den;
                best_failures = failures;
            }
        };
        UInt32 best_e = 255;
        for (UInt32 e = 0; e <= FloatTraits<F>::MAX_DECIMAL; ++e)
        {
            bool present = false;
            UInt32 failures = 0;
            for (UInt32 i = 0; i < ns; ++i)
            {
                present |= scale[i] == e;
                failures += scale[i] > e;
            }
            if (!present)
                continue;
            double before = best_cost;
            considerDen(POW10[e], failures);
            if (best_cost < before)
                best_e = e;
        }
        if (best_e != 255 && best_e <= FloatTraits<F>::SUSPICIOUS_DECIMAL && best_failures * 20 <= ns)
            return best_den;

        /// Not a clean short decimal: look for another common denominator.
        auto countFailures = [&](double den)
        {
            UInt32 failures = 0;
            for (UInt32 i = 0; i < ns; ++i)
            {
                Int64 q;
                failures += !toRational(sample[i], den, q);
            }
            return failures;
        };
        /// 1. The least common multiple of the minimal denominators (continued fractions) that repeat.
        {
            double dens[SAMPLE];
            UInt32 nd = 0;
            for (UInt32 i = 0; i < ns; ++i)
            {
                double q = minimalDenominator(sample[i], FloatTraits<F>::MAX_DENOMINATOR);
                if (q >= 1)
                    dens[nd++] = q;
            }
            std::sort(dens, dens + nd);
            double l = 1;
            bool ok = nd * 2 >= ns;
            for (UInt32 i = 0; i < nd && ok;)
            {
                UInt32 j = i;
                while (j < nd && dens[j] == dens[i])
                    ++j;
                if (j - i >= 2 || nd < 8)
                {
                    UInt64 g = std::gcd(UInt64(l), UInt64(dens[i]));
                    l = l / double(g) * dens[i];
                    if (l > FloatTraits<F>::MAX_DENOMINATOR)
                        ok = false;
                }
                i = j;
            }
            if (ok && l > 1)
                considerDen(l, countFailures(l));
        }
        /// 2. A small multiple of a power of ten: averages over 3, 12, 60 samples and the like. The type may
        ///    lack the precision to reveal such a denominator on a single value, but a sample does.
        {
            UInt32 quick = std::min<UInt32>(ns, 6);
            for (UInt32 e = 0; e <= FloatTraits<F>::SUSPICIOUS_DECIMAL; ++e)
            {
                for (UInt32 m : DENOMINATOR_MULTIPLIERS)
                {
                    double den = POW10[e] * m;
                    if (den > FloatTraits<F>::MAX_DENOMINATOR)
                        break;
                    /// Cheap rejection on a few values first.
                    bool pass = true;
                    for (UInt32 i = 0; i < quick && pass; ++i)
                    {
                        Int64 q;
                        pass = toRational(sample[i * (ns / quick)], den, q);
                    }
                    if (!pass)
                        continue;
                    UInt32 failures = countFailures(den);
                    if (failures * 10 <= ns)
                        considerDen(den, failures);
                }
            }
        }
        return best_den;
    }

    /// Chooses how the values are reconstructed from the integers: by a division or by one of the multiplications.
    static void chooseReconstruction(const F * sample, UInt32 ns, double den, bool & mul, bool & single, double & scale)
    {
        /// In the order of preference at equal exactness on the sample: a division reproduces correctly rounded
        /// decimals, so it is preferred to a multiplication; single precision is faster than double.
        struct Variant
        {
            bool mul;
            bool single;
            double scale;
        };
        Variant variants[5];
        UInt32 nvar = 0;
        if (sizeof(F) == 4)
            variants[nvar++] = {false, true, den};
        variants[nvar++] = {false, false, den};
        if (den != 1)
        {
            if (sizeof(F) == 4)
                variants[nvar++] = {true, true, double(Float32(1.0 / den))};
            variants[nvar++] = {true, false, 1.0 / den};
            if (sizeof(F) == 4)
                variants[nvar++] = {true, false, double(Float32(1.0 / den))};
        }
        UInt32 best_exact = 0;
        mul = variants[0].mul;
        single = variants[0].single;
        scale = variants[0].scale;
        for (UInt32 c = 0; c < nvar; ++c)
        {
            UInt32 exact = 0;
            for (UInt32 i = 0; i < ns; ++i)
            {
                Int64 q;
                if (integerCandidate(sample[i], den, q))
                    exact += floatBits(reconstruct<F>(q, variants[c].mul, variants[c].single, variants[c].scale)) == floatBits(sample[i]);
            }
            if (c == 0 || exact > best_exact)
            {
                best_exact = exact;
                mul = variants[c].mul;
                single = variants[c].single;
                scale = variants[c].scale;
            }
        }
    }

    /// Turns the few values that need an adjustment into exceptions, so that the chunk needs no adjustment stream.
    void demoteInexact(UInt32 n, Converted & c)
    {
        std::vector<Int64> lat;
        std::vector<UInt16> exc;
        lat.reserve(c.lat.size());
        exc.reserve(c.exc_pos.size() + c.inexact.size());
        size_t e = 0;
        size_t x = 0;
        size_t ri = 0;
        for (UInt32 pos = 0; pos < n; ++pos)
        {
            if (e < c.exc_pos.size() && c.exc_pos[e] == pos)
            {
                exc.push_back(UInt16(pos));
                ++e;
                continue;
            }
            if (x < c.inexact.size() && c.inexact[x] == pos)
            {
                exc.push_back(UInt16(pos));
                ++x;
                ++ri;
                continue;
            }
            lat.push_back(c.lat[ri++]);
        }
        c.lat.swap(lat);
        c.exc_pos.swap(exc);
        c.inexact.clear();
        c.adj.assign(c.lat.size(), 0);
    }

    void convert(const UInt8 * src, UInt32 n, double den, bool mul, bool single, double scale, Converted & c)
    {
        c.lat.clear();
        c.adj.clear();
        c.exc_pos.clear();
        c.inexact.clear();
        c.mul = mul;
        c.single = single;
        c.scale = scale;
        c.den = den;
        c.lat.reserve(n);
        c.adj.reserve(n);
        for (UInt32 i = 0; i < n; ++i)
        {
            F v = loadFloat<F>(src + size_t(i) * sizeof(F));
            Int64 q;
            if (!integerCandidate(v, den, q))
            {
                c.exc_pos.push_back(UInt16(i));
                continue;
            }
            F r = reconstruct<F>(q, mul, single, scale);
            U vb = floatBits(v);
            U rb = floatBits(r);
            Int64 a = 0;
            if (vb != rb)
            {
                U d = U(orderedBits<F>(vb) - orderedBits<F>(rb));
                a = sizeof(F) == 4 ? Int64(Int32(d)) : Int64(d);
                c.inexact.push_back(UInt16(i));
            }
            c.lat.push_back(q);
            c.adj.push_back(a);
        }
    }

    /// The residuals of the plan for the latent, into `resid`; fills the initial values of the predictor.
    UInt32 computeResiduals(const std::vector<Int64> & lat, const Plan & plan, ChunkParams & p, std::vector<Int64> & out)
    {
        UInt32 n = UInt32(lat.size());
        p.order = plan.order;
        p.center = plan.center;
        out.resize(n);
        if (p.order == 0)
        {
            std::copy(lat.begin(), lat.end(), out.begin());
            return n;
        }
        std::vector<Int64> & a = diff[0];
        std::vector<Int64> & b = diff[1];
        a.assign(lat.begin(), lat.end());
        p.initial[0] = a[0];
        for (UInt32 order = 1; order <= p.order; ++order)
        {
            std::vector<Int64> & from = (order & 1) ? a : b;
            std::vector<Int64> & to = (order & 1) ? b : a;
            to.resize(n);
            for (UInt32 i = order; i < n; ++i)
                to[i] = Int64(UInt64(from[i]) - UInt64(from[i - 1]));
            if (order < p.order)
                p.initial[order] = to[order];
            if (order == p.order)
                for (UInt32 i = order; i < n; ++i)
                    out[i - order] = Int64(UInt64(to[i]) - UInt64(p.center));
        }
        return n - p.order;
    }

    /// Base (a sampled median, an actual element) and the common divisor of the differences from it.
    static void prepareLatent(std::vector<Int64> & lat, ChunkParams & p, bool with_gcd)
    {
        UInt32 n = UInt32(lat.size());
        Int64 sample[255];
        UInt32 ns = 0;
        UInt32 step = std::max<UInt32>(1, n / 255);
        for (UInt32 i = 0; i < n && ns < 255; i += step)
            sample[ns++] = lat[i];
        std::nth_element(sample, sample + ns / 2, sample + ns);
        p.base = sample[ns / 2];
        UInt64 g = 0;
        if (with_gcd)
        {
            for (UInt32 i = 0; i < n && g != 1; ++i)
                g = std::gcd(g, magnitude(Int64(UInt64(lat[i]) - UInt64(p.base))));
        }
        if (g > 1)
        {
            p.gcd = g;
            for (UInt32 i = 0; i < n; ++i)
                lat[i] = (lat[i] - p.base) / Int64(g);
        }
        else
        {
            p.gcd = 1;
            for (UInt32 i = 0; i < n; ++i)
                lat[i] = Int64(UInt64(lat[i]) - UInt64(p.base));
        }
    }

    UInt32 sampleValues(const UInt8 * src, UInt32 n, const UInt16 * positions, UInt32 count, F * sample)
    {
        UInt32 ns = 0;
        UInt32 total = positions ? count : n;
        UInt32 step = std::max<UInt32>(1, total / SAMPLE);
        /// Zeros and non-finite values say nothing about the denominator: take the next informative value of
        /// every stride instead, so that sparse data still gives a full sample.
        for (UInt32 start = 0; start < total && ns < SAMPLE; start += step)
        {
            UInt32 stop = std::min(total, start + step);
            for (UInt32 k = start; k < stop; ++k)
            {
                F v = loadFloat<F>(src + size_t(positions ? positions[k] : k) * sizeof(F));
                if (v == 0 || !std::isfinite(v))
                    continue;
                sample[ns++] = v;
                break;
            }
        }
        return ns;
    }

    /// Encodes one chunk of n values (raw little-endian floats) and returns the number of bytes written.
    /// `dst` must have room for n * sizeof(F) + 64 bytes.
    size_t encodeChunk(const UInt8 * src, UInt32 n, UInt8 * dst)
    {
        const size_t raw_size = 1 + size_t(n) * sizeof(F);

        auto writeRaw = [&]
        {
            dst[0] = MODE_RAW;
            copyBytes(dst + 1, src, size_t(n) * sizeof(F));
            return raw_size;
        };

        /// --- First pass: constant check, common trailing zeros, number of distinct values (linear counting).
        U first = loadBits<F>(src);
        U orall = 0;
        bool all_equal = true;
        constexpr UInt32 BITMAP_BITS = 1u << 16;
        bitmap.assign(BITMAP_BITS / 64, 0);
        for (UInt32 i = 0; i < n; ++i)
        {
            U b = loadBits<F>(src + size_t(i) * sizeof(F));
            all_equal &= b == first;
            orall |= b;
            UInt32 h = UInt32((UInt64(b) * 0x9E3779B97F4A7C15ull) >> 48);
            bitmap[h >> 6] |= UInt64(1) << (h & 63);
        }
        if (all_equal)
        {
            dst[0] = MODE_CONST;
            storeBits<F>(dst + 1, first);
            return 1 + sizeof(F);
        }
        UInt32 set_bits = 0;
        for (UInt64 word : bitmap)
            set_bits += std::popcount(word);
        double distinct = set_bits >= BITMAP_BITS ? double(n)
            : -double(BITMAP_BITS) * std::log(1.0 - double(set_bits) / BITMAP_BITS);
        distinct = std::min(distinct, double(n));

        /// --- DIV / MUL front-end: find the denominator on a sample.
        ChunkParams pd;
        bool have_div = false;
        {
            F sample[SAMPLE];
            UInt32 ns = sampleValues(src, n, nullptr, 0, sample);
            double den = ns ? detectDenominator(sample, ns) : 1;
            if (den > 0)
            {
                bool mul;
                bool single;
                double scale;
                chooseReconstruction(sample, ns, den, mul, single, scale);
                convert(src, n, den, mul, single, scale, conv);
                /// Many inexact values: the sample may have missed a finer denominator. Derive it from them.
                if (conv.inexact.size() * 50 > n && den == std::floor(den))
                {
                    ns = sampleValues(src, n, conv.inexact.data(), UInt32(conv.inexact.size()), sample);
                    double den2 = ns >= 4 ? detectDenominator(sample, ns) : 0;
                    if (den2 > 0 && den2 != den && den2 == std::floor(den2))
                    {
                        UInt64 g = std::gcd(UInt64(den), UInt64(den2));
                        double l = den / double(g) * den2;
                        if (l <= FloatTraits<F>::MAX_DENOMINATOR)
                        {
                            F inexact_sample[SAMPLE];
                            const UInt32 num_inexact = ns;
                            std::copy(sample, sample + ns, inexact_sample);
                            ns = sampleValues(src, n, nullptr, 0, sample);
                            chooseReconstruction(sample, ns, l, mul, single, scale);
                            /// The finer denominator is kept if it halves the number of inexact values: check
                            /// on the sample that it has a chance before converting everything again.
                            UInt32 fixed = 0;
                            for (UInt32 i = 0; i < num_inexact; ++i)
                            {
                                Int64 q;
                                if (integerCandidate(inexact_sample[i], l, q))
                                    fixed += floatBits(reconstruct<F>(q, mul, single, scale)) == floatBits(inexact_sample[i]);
                            }
                            if (num_inexact < 32 || fixed * 3 >= num_inexact)
                            {
                                std::swap(conv, conv_saved);
                                convert(src, n, l, mul, single, scale, conv);
                                if (conv.inexact.size() * 2 > conv_saved.inexact.size())
                                    std::swap(conv, conv_saved);
                            }
                        }
                    }
                }
                /// A few inexact values are cheaper as exceptions than as a stream of adjustments to decode.
                if (!conv.inexact.empty() && conv.inexact.size() * 64 <= n)
                    demoteInexact(n, conv);
                have_div = !conv.lat.empty();
            }
        }
        const UInt32 nexc = have_div ? UInt32(conv.exc_pos.size()) : 0;
        const UInt32 nreg = n - nexc;

        /// --- Candidates: (front-end, order), ranked by a cheap proxy; the tokens of the best few are counted.
        std::vector<Candidate> candidates;
        std::vector<Plan> plans;
        double div_side_bits = 0;
        double div_exception_bits = 0;
        const bool have_adj = have_div && !conv.inexact.empty();
        /// A few distinct values: they become dictionary symbols. Differencing destroys them, so it is not
        /// considered unless it helps even without a dictionary (runs, a smooth trend).
        const bool dictionary_like = distinct * 8 <= n;
        auto addPlans = [&](bool div, double side_bits)
        {
            for (const Plan & plan : plans)
            {
                if (dictionary_like && plan.order && plan.proxy > plans[0].class_proxy * 0.9)
                    continue;
                candidates.push_back({div, plan, plan.proxy + side_bits});
            }
        };
        if (have_div)
        {
            pd.front = conv.mul ? FRONT_MUL : FRONT_DIV;
            pd.single = conv.single;
            pd.scale = conv.scale;
            prepareLatent(conv.lat, pd, true);
            double adj_proxy = have_adj ? proxyOf(conv.adj.data(), nreg) + 64 : 0;
            double exc_proxy = double(nexc) * (W + 8.0);
            choosePredictor(conv.lat.data(), nreg, distinct, plans);
            addPlans(true, adj_proxy + exc_proxy);
            div_side_bits = exc_proxy;
            div_exception_bits = exc_proxy;
        }
        /// The bit patterns are worth trying unless every value is reproduced from an integer exactly, or all but
        /// a few exceptions which do not make a dictionary.
        ChunkParams pb;
        bool narrow = false;
        int unused_exp = 0;
        if (have_div && !have_adj && (!nexc || (!dictionary_like && nexc * 64 <= n)))
        {
            UInt64 widest = 0;
            for (Int64 q : conv.lat)
                widest |= magnitude(q);
            UInt64 span = widest * pd.gcd + magnitude(pd.base);
            /// Binary fractions are served better by the bit patterns: a power-of-two denominator wastes bits
            /// on the values of higher binades.
            bool binary = pd.scale > 1 && std::frexp(pd.scale, &unused_exp) == 0.5;
            narrow = !binary && bitLength(span) + 3 <= FloatTraits<F>::MANTISSA_BITS;
        }
        if (!narrow)
        {
            pb.front = FRONT_BITS;
            pb.shift = UInt8(std::countr_zero(orall));
            lat_bits.resize(n);
            for (UInt32 i = 0; i < n; ++i)
            {
                U b = loadBits<F>(src + size_t(i) * sizeof(F));
                lat_bits[i] = Int64(UInt64(orderedBits<F>(b) >> pb.shift));
            }
            prepareLatent(lat_bits, pb, false);
            plans.clear();
            choosePredictor(lat_bits.data(), n, distinct, plans);
            addPlans(false, 0);
        }
        std::sort(candidates.begin(), candidates.end(), [](const Candidate & a, const Candidate & b) { return a.cost < b.cost; });

        struct Evaluated
        {
            double bits = 1e300;
            ChunkParams params;
            bool div = false;
            StreamEncoder * stream = nullptr;
        };
        Evaluated evals[MAX_CANDIDATES];
        UInt32 evaluated = 0;
        bool adj_prepared = false;
        for (const Candidate & c : candidates)
        {
            /// The proxy is crude: look at everything that is not hopeless against the best proxy.
            if (evaluated >= 1 && c.cost > candidates[0].cost * PROXY_BAND + 64)
                break;
            if (evaluated >= MAX_CANDIDATES)
                break;
            ChunkParams p = c.div ? pd : pb;
            const std::vector<Int64> & lat = c.div ? conv.lat : lat_bits;
            /// Without differencing the residuals are the latent itself.
            const Int64 * residuals = lat.data();
            UInt32 nres = UInt32(lat.size());
            if (c.plan.order)
            {
                nres = computeResiduals(lat, c.plan, p, resid[evaluated]);
                residuals = resid[evaluated].data();
            }
            /// The analysis chooses the bins on a sample; the counts of the tokens then give the size within
            /// about a percent, short of the gain of the contexts.
            StreamEncoder & s = streams[evaluated];
            s.analyze(residuals, nres, true, 1);
            s.count();
            double bits = s.counted_bits + 64.0 * p.order;
            if (c.div)
            {
                if (have_adj && !adj_prepared)
                {
                    /// The adjustments are counted now, and completed only if they are going to be written.
                    adj_stream.analyze(conv.adj.data(), nreg, false, 0);
                    adj_stream.count();
                    div_side_bits += adj_stream.counted_bits;
                    adj_prepared = true;
                }
                bits += div_side_bits;
            }
            evals[evaluated++] = {bits, p, c.div, &s};
        }
        std::sort(evals, evals + evaluated, [](const Evaluated & a, const Evaluated & b) { return a.bits < b.bits; });
        bool adj_completed = false;
        auto exactBits = [&](Evaluated & e)
        {
            e.stream->complete();
            double bits = e.stream->estimated_bits + modelBits(*e.stream) + 64.0 * e.params.order;
            if (e.div)
            {
                bits += div_exception_bits;
                if (have_adj)
                {
                    if (!adj_completed)
                        adj_stream.complete();
                    adj_completed = true;
                    bits += adj_stream.estimated_bits + modelBits(adj_stream);
                }
            }
            return bits;
        };
        /// All the work is done for the best candidate, and for the second one only when the two are very close.
        UInt32 best = 0;
        double exact_best = exactBits(evals[0]);
        if (evaluated > 1 && evals[1].bits <= evals[0].bits * VERY_CLOSE_CALL && exactBits(evals[1]) < exact_best)
            best = 1;
        StreamEncoder & stream = *evals[best].stream;
        ChunkParams & p = evals[best].params;
        const bool best_div = evals[best].div;
        p.has_adj = best_div && have_adj;

        /// --- Exceptions: verbatim, or entropy-coded when there are enough of them for the models to pay off.
        bool coded_exceptions = false;
        if (best_div && nexc)
        {
            p.exceptions = nexc;
            p.exc_kind = EXC_RAW;
            if (nexc >= 16)
            {
                gaps.resize(nexc);
                exc_values.resize(nexc);
                Int64 sample[255];
                UInt32 ns = 0;
                UInt32 step = std::max<UInt32>(1, nexc / 255);
                UInt32 prev = 0;
                for (UInt32 k = 0; k < nexc; ++k)
                {
                    UInt32 at = conv.exc_pos[k];
                    gaps[k] = at - prev;
                    prev = at + 1;
                    U b = loadBits<F>(src + size_t(at) * sizeof(F));
                    exc_values[k] = Int64(UInt64(orderedBits<F>(b)));
                    if (k % step == 0 && ns < 255)
                        sample[ns++] = exc_values[k];
                }
                std::nth_element(sample, sample + ns / 2, sample + ns);
                p.exc_base = sample[ns / 2];
                for (UInt32 k = 0; k < nexc; ++k)
                    exc_values[k] = Int64(UInt64(exc_values[k]) - UInt64(p.exc_base));
                gap_stream.analyze(gaps.data(), nexc, false, 0);
                gap_stream.finalize();
                exc_stream.analyze(exc_values.data(), nexc, false, 0);
                exc_stream.finalize();
                double coded = gap_stream.estimated_bits + modelBits(gap_stream) + exc_stream.estimated_bits + modelBits(exc_stream) + 72;
                if (coded < double(nexc) * (16.0 + W))
                {
                    coded_exceptions = true;
                    p.exc_kind = EXC_CODED;
                }
            }
        }

        /// --- Assemble the chunk.
        scratch.resize(size_t(n) * 10 + size_t(MAX_ALPHABET) * 160 + 65536);
        BitWriter w(scratch.data());
        w.put(MODE_ENTROPY, 2);
        writeChunkParams(w, p);
        stream.write(w, payload_main);
        payload_adj.clear();
        payload_gap.clear();
        payload_exc.clear();
        if (p.has_adj)
            adj_stream.write(w, payload_adj);
        if (coded_exceptions)
        {
            gap_stream.write(w, payload_gap);
            exc_stream.write(w, payload_exc);
        }
        UInt8 * end = w.finish();
        if (p.exc_kind == EXC_RAW)
        {
            for (UInt16 pos : conv.exc_pos)
            {
                unalignedStoreLittleEndian<UInt16>(end, pos);
                end += 2;
            }
            for (UInt16 pos : conv.exc_pos)
            {
                copyBytes(end, src + size_t(pos) * sizeof(F), sizeof(F));
                end += sizeof(F);
            }
        }
        size_t header_size = size_t(end - scratch.data());
        size_t total = header_size + payload_main.size() + payload_adj.size() + payload_gap.size() + payload_exc.size();
        if (total >= raw_size)
            return writeRaw();

        UInt8 * out = dst;
        copyBytes(out, scratch.data(), header_size);
        out += header_size;
        for (const std::vector<UInt8> * part : {&payload_main, &payload_adj, &payload_gap, &payload_exc})
        {
            copyBytes(out, part->data(), part->size());
            out += part->size();
        }

        return total;
    }

    /// Compresses a block; returns the compressed size (at most size + 1). `dst` must have room for size + 1 bytes.
    size_t compress(const UInt8 * src, size_t size, UInt8 * dst)
    {
        size_t n = size / sizeof(F);
        size_t tail = size % sizeof(F);
        /// Every chunk falls back to its raw form (one byte more than the values) when it does not compress.
        auto out = std::make_unique_for_overwrite<UInt8[]>(size + (n / CHUNK_MAX + 2) * 80 + 64);
        UInt8 * p = out.get();
        *p++ = FORMAT_VERSION | (sizeof(F) == 8 ? BLOCK_FLAG_WIDE : 0);
        for (size_t off = 0; off < n; off += CHUNK_MAX)
        {
            UInt32 cn = UInt32(std::min<size_t>(CHUNK_MAX, n - off));
            p += encodeChunk(src + off * sizeof(F), cn, p);
        }
        copyBytes(p, src + n * sizeof(F), tail);
        p += tail;
        size_t total = size_t(p - out.get());
        if (total > size)
        {
            dst[0] = FORMAT_VERSION | (sizeof(F) == 8 ? BLOCK_FLAG_WIDE : 0) | BLOCK_FLAG_RAW;
            copyBytes(dst + 1, src, size);
            return size + 1;
        }
        copyBytes(dst, out.get(), total);
        return total;
    }
};

}

UInt32 compress(const char * source, UInt32 source_size, char * dest, UInt8 float_width)
{
    const auto * src = reinterpret_cast<const UInt8 *>(source);
    auto * dst = reinterpret_cast<UInt8 *>(dest);
    /// The encoder and the decoder keep a few megabytes of scratch buffers and tables. They are cached per thread:
    /// allocating and zeroing them for every block of 64 KiB costs more than a half of the compression time.
    /// Every use initializes what it reads, so an exception thrown in the middle leaves a usable object.
    if (float_width == sizeof(Float64))
    {
        thread_local std::unique_ptr<Encoder<Float64>> encoder;
        if (!encoder)
            encoder = std::make_unique<Encoder<Float64>>();
        return static_cast<UInt32>(encoder->compress(src, source_size, dst));
    }
    if (float_width == sizeof(Float32))
    {
        thread_local std::unique_ptr<Encoder<Float32>> encoder;
        if (!encoder)
            encoder = std::make_unique<Encoder<Float32>>();
        return static_cast<UInt32>(encoder->compress(src, source_size, dst));
    }
    logicalError("unsupported float width");
}

void decompress(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
{
    if (source_size < 1)
        corrupt("empty block");
    const UInt8 head = static_cast<UInt8>(source[0]);
    if ((head & 0x0f) != FORMAT_VERSION)
        corrupt("unknown format version");
    if (head & 0xc0)
        corrupt("reserved bits are set");
    auto * dst = reinterpret_cast<UInt8 *>(dest);
    if (head & BLOCK_FLAG_RAW)
    {
        if (source_size != UInt64(uncompressed_size) + 1)
            corrupt("bad size of a block stored verbatim");
        copyBytes(dst, source + 1, uncompressed_size);
        return;
    }

    /// The decoder reads the bit streams with unaligned 64-bit loads that may start at the last byte:
    /// give it a copy of the data followed by zeros instead of relying on what is after the source.
    constexpr size_t PADDING = 16;
    auto padded = std::make_unique_for_overwrite<UInt8[]>(size_t(source_size) + PADDING);
    copyBytes(padded.get(), source, source_size);
    std::memset(padded.get() + source_size, 0, PADDING);

    if (head & BLOCK_FLAG_WIDE)
    {
        thread_local std::unique_ptr<Decoder<Float64>> decoder;
        if (!decoder)
            decoder = std::make_unique_for_overwrite<Decoder<Float64>>();
        decoder->decompress(padded.get(), source_size, dst, uncompressed_size);
    }
    else
    {
        thread_local std::unique_ptr<Decoder<Float32>> decoder;
        if (!decoder)
            decoder = std::make_unique_for_overwrite<Decoder<Float32>>();
        decoder->decompress(padded.get(), source_size, dst, uncompressed_size);
    }
}

}

}
