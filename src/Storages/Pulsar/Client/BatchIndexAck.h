#pragma once

#include <cstdint>
#include <mutex>
#include <vector>

namespace DB::Pulsar
{

/// Tracks which messages of a single batched entry are still unacknowledged.
///
/// A Pulsar batch is one broker entry `(ledgerId, entryId)` that carries several messages. To
/// acknowledge one message of a batch individually, the client sends an `ACK` for the entry together
/// with an `ack_set`: the bitmap of the messages that are still outstanding. The broker considers the
/// whole entry acknowledged only once the bitmap is empty. The encoding matches the Java/C++ clients:
/// a `BitSet` serialized to an array of 64-bit words (bit `i` lives in word `i / 64`, position `i % 64`).
///
/// All the message ids of one batch share a single `BatchIndexAck`, so clearing a bit is visible to
/// every sibling message id when its own `ack_set` is built.
class BatchIndexAck
{
public:
    explicit BatchIndexAck(int32_t batch_size)
        : words((static_cast<size_t>(batch_size) + 63) / 64, ~uint64_t(0))
    {
        /// Clear the padding bits above `batch_size` in the last word, so an all-acknowledged batch
        /// serializes to an empty `ack_set`.
        const size_t total_bits = words.size() * 64;
        for (size_t bit = static_cast<size_t>(batch_size); bit < total_bits; ++bit)
            words[bit / 64] &= ~(uint64_t(1) << (bit % 64));
    }

    /// Mark one index as acknowledged. Returns true once no index remains outstanding.
    bool ackIndex(int32_t index)
    {
        std::lock_guard lock(mutex);
        if (index >= 0 && static_cast<size_t>(index) / 64 < words.size())
            words[index / 64] &= ~(uint64_t(1) << (index % 64));
        return isEmptyLocked();
    }

    /// The outstanding set, as the array of words Pulsar expects in `ack_set`. Trailing zero words
    /// are trimmed; an empty result means the whole entry can be acknowledged.
    std::vector<int64_t> outstanding() const
    {
        std::lock_guard lock(mutex);
        size_t in_use = words.size();
        while (in_use > 0 && words[in_use - 1] == 0)
            --in_use;
        std::vector<int64_t> result(in_use);
        for (size_t i = 0; i < in_use; ++i)
            result[i] = static_cast<int64_t>(words[i]);
        return result;
    }

private:
    bool isEmptyLocked() const
    {
        for (uint64_t word : words)
            if (word != 0)
                return false;
        return true;
    }

    mutable std::mutex mutex;
    std::vector<uint64_t> words;
};

}
