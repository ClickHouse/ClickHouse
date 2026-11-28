#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HyperLogLogCounter.h>
#include <base/types.h>

#include <algorithm>
#include <bit>

namespace DB
{

/// Counts the distinct keys of one adaptive aggregation thread approximately: the keys that its tables insert
/// and the keys that it stages. A frozen table stages the keys that it misses instead of inserting them, so
/// its size stops counting the thread's keys at the freeze. The counter keeps counting them, so a thread
/// under a throw-mode `max_rows_to_group_by` can throw while it reads instead of at the merge.
///
/// The counter takes the hash of a key in the thread's table, which is also the routing hash of the key's
/// staged records, so a key counts once whether a table inserted it, a frozen table staged it, or both. It
/// mixes the hash first, because the bits of a table hash can depend on the data: `HashCRC32` of an integer
/// is linear, so the keys that pass a filter on the parity of some of their bits can all share the low bit
/// of their hashes. The low `sample_bits` bits of the mixed hash choose the sample, one key in
/// 2^sample_bits, and the HyperLogLog reads the bits above them. The sample shrinks as `precise_count`, the
/// count that the caller compares the estimate with, grows, so a thread at that count has 2^16 to 2^17
/// sampled keys: every key up to 131071, one in 8 at 10^6, one in 2^17 at 10^10. The estimate of such a
/// count then has a standard error of about 0.9%: 0.8% from the 2^14 buckets of the HyperLogLog and 0.4%
/// from the sample.
class AdaptiveDistinctKeyCounter
{
public:
    explicit AdaptiveDistinctKeyCounter(UInt64 precise_count)
        : error_margin(precise_count / 16)
        , sample_bits(std::max(0, std::bit_width(precise_count) - 1 - min_sample_bits))
        , sample_mask((UInt64{1} << sample_bits) - 1)
    {
    }

    /// The caller passes the hash of a key in the thread's table. It may be 32-bit.
    void add(UInt64 hash)
    {
        const UInt64 mixed = intHash64(hash);
        if ((mixed & sample_mask) == 0)
            sampled_keys.insert(mixed >> sample_bits);
    }

    [[nodiscard]] UInt64 getCount() const { return sampled_keys.size() << sample_bits; }

    /// The error that the estimate of a count near `precise_count` practically never exceeds: a sixteenth of
    /// the count, about seven standard errors. A count that exceeds another by more than the margin means
    /// more keys.
    [[nodiscard]] UInt64 getErrorMargin() const { return error_margin; }

private:
    /// A thread at `precise_count` keys counts at least 2^min_sample_bits sampled keys.
    static constexpr int min_sample_bits = 16;

    const UInt64 error_margin;
    const int sample_bits;
    /// The low `sample_bits` bits of a mixed hash, all zero for a key in the sample.
    const UInt64 sample_mask;
    HyperLogLogCounter<14, UInt64, TrivialHash, UInt64> sampled_keys;
};

}
