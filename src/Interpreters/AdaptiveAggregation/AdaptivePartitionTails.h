#pragma once

#include <memory>

#include <base/defines.h>
#include <base/types.h>
#include <Common/PODArray.h>
#include <Interpreters/AdaptiveAggregation/StagedRecords.h>

namespace DB
{

/// The tails of a frozen producer's partitions, which the producer holds through this base because their
/// entries are typed by the record key of the aggregation (see `AdaptivePartitionTails`).
class IAdaptivePartitionTails
{
public:
    virtual ~IAdaptivePartitionTails() = default;
};

/// The tail of a partition is the last record that a frozen producer staged into it. A miss whose key equals
/// the key of its partition's tail combines into the tail instead of staging a record of its own (see
/// `AdaptiveMissCombiner`). The tails outlive blocks, so a row combines whenever no other key of its
/// partition was staged since its key's last record, wherever the block boundaries fall. For example, the
/// misses A, B, A stage three records when A and B share a partition, and two when they do not, because the
/// second A then combines into the first.
///
/// A tail is pending while its record is still a descriptor of the producer's batch of misses, and written
/// once the writer appended the record (see `Aggregator::appendDelayedRecords`), which then reports where it
/// put the records of the batch (see `recordsWritten`). Only the writer appends, in the order of the batch,
/// so a pending tail is its partition's last descriptor of the batch, and a written tail is its partition's
/// last record, which only it may roll back (see `PartitionedRecordBuffer::rollback`). Every staging writes
/// the whole batch, so no pending tail outlives the block of its descriptor.
///
/// A tail keeps its key as the kernel probes it: a fixed-width key by value, and a string-like key as a view
/// into the block while pending and into its record once written. The tails belong to one frozen phase and
/// one fill of the producer's records: the producer drops them when it freezes and when it spills its
/// records.
///
/// Every miss of a kernel whose records combine looks its partition's tail up, while the probes of the frozen
/// table keep evicting the cache. The hashes and the states of the tails are therefore kept apart from the
/// rest, so the lookup of a miss that matches no tail reads an array of 8 bytes per partition, 8 KiB for 1024
/// partitions, which stays in the cache. The rest of a tail is read only when the hashes match.
template <typename RecordKey>
class AdaptivePartitionTails final : public IAdaptivePartitionTails
{
public:
    /// What a tail holds besides its hash and its state.
    struct Tail
    {
        RecordKey key{};
        union
        {
            /// The descriptor of a pending tail in the batch.
            UInt32 descriptor;
            /// Where the record of a written tail keeps the number of rows it stands for, or null for a
            /// record without one: the start of a count record, whose format grows its count (see
            /// `addToCount`), or the `UInt32` multiplicity of the record of a row selection that counts rows.
            char * counter = nullptr;
        };
        /// The rank of the record's row under a row selection (see `fillRowSelectionRanks`).
        Int64 rank = 0;
        /// The size of the record of a written tail and of the key it stores, which its rollback gives back to
        /// the buffer and to the accounting of the staged bytes (see `AdaptiveStagedBytes`).
        UInt32 bytes = 0;
        UInt32 key_bytes = 0;
    };

    explicit AdaptivePartitionTails(size_t num_partitions_)
        : num_partitions(num_partitions_)
        , hashes(std::make_unique<UInt64[]>(num_partitions_))
        , states(std::make_unique<State[]>(num_partitions_))
        , tails(std::make_unique<Tail[]>(num_partitions_))
    {
    }

    /// The tail of the partition when it is a record of `key` with `hash`, or null. The key of a pending tail
    /// is compared only when `pending_keys_comparable`: a kernel whose key views do not outlive their row
    /// keeps no pending keys.
    Tail * ALWAYS_INLINE find(size_t partition, UInt64 hash, const RecordKey & key, bool pending_keys_comparable)
    {
        if (hashes[partition] != hash)
            return nullptr;
        const State state = states[partition];
        if (state == State::empty || (state == State::pending && !pending_keys_comparable))
            return nullptr;
        Tail & tail = tails[partition];
        return tail.key == key ? &tail : nullptr;
    }

    bool ALWAYS_INLINE isPending(size_t partition) const { return states[partition] == State::pending; }

    /// Adds `rows` to the count of `tail`, a written count record, while the count keeps its stored width,
    /// and returns whether it did.
    [[nodiscard]] bool ALWAYS_INLINE addToCount(const Tail & tail, UInt32 rows) const
    {
        return add_to_count(tail.counter, rows);
    }

    /// The tail of the partition. The caller guarantees that it holds a record.
    Tail & ALWAYS_INLINE at(size_t partition) { return tails[partition]; }

    /// Makes descriptor `descriptor` of the batch, a record of `key` with `hash` and `rank`, the pending tail
    /// of the partition.
    void ALWAYS_INLINE setPending(size_t partition, UInt32 descriptor, UInt64 hash, const RecordKey & key, Int64 rank)
    {
        hashes[partition] = hash;
        states[partition] = State::pending;
        Tail & tail = tails[partition];
        tail.key = key;
        tail.descriptor = descriptor;
        tail.rank = rank;
    }

    /// The positions where the writer puts the records of a batch of `records` descriptors, which
    /// `recordsWritten` reads.
    char ** recordPositions(size_t records)
    {
        positions.resize(records);
        return positions.data();
    }

    /// Turns every pending tail into the record that the writer appended for its descriptor in `format`, at
    /// the position it put into `recordPositions`. A count record keeps its rows in its count, which the
    /// format grows (see `addToCount`), and the record of a row selection that counts rows in its
    /// multiplicity (see `AdaptiveArgumentLayout::multiplicity_offset`). The states take a byte per
    /// partition, so a pass over all of them costs less than keeping a list of the pending ones in the
    /// kernel.
    template <typename Format>
    void recordsWritten(const Format & format, const AdaptiveArgumentLayout * layout)
    {
        if constexpr (Format::with_count)
            add_to_count = &Format::addToCount;
        for (size_t partition = 0; partition < num_partitions; ++partition)
        {
            if (states[partition] != State::pending)
                continue;
            Tail & tail = tails[partition];
            chassert(tail.descriptor < positions.size());
            char * record = positions[tail.descriptor];
            if constexpr (adaptive_key_stages_bytes<RecordKey>)
                tail.key = format.readKey(record, hashes[partition]);
            tail.counter = nullptr;
            if constexpr (Format::with_count)
                tail.counter = record;
            else if constexpr (Format::with_arguments)
            {
                if (layout->multiplicity_offset)
                    tail.counter = layout->multiplicityPosition(record);
            }
            tail.bytes = static_cast<UInt32>(format.bytes(record));
            tail.key_bytes = static_cast<UInt32>(format.key(record).second);
            states[partition] = State::written;
        }
    }

private:
    enum class State : UInt8
    {
        empty,
        pending,
        written,
    };

    const size_t num_partitions;
    /// Arrays rather than vectors: hardened libc++ checks the bounds of every `std::vector` index, which
    /// every miss of the kernel would pay several times. The states start empty.
    std::unique_ptr<UInt64[]> hashes;
    std::unique_ptr<State[]> states;
    std::unique_ptr<Tail[]> tails;
    PaddedPODArray<char *> positions;
    /// The `addToCount` of the format of count records, which `recordsWritten` takes from the format.
    bool (*add_to_count)(char * record, UInt32 rows) = nullptr;
};

}
