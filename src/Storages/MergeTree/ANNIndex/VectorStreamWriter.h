#pragma once

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/DiskANNFbinWriter.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMapWriter.h>
#include <Storages/StorageSnapshot.h>

#include <Core/Types.h>
#include <Common/Logger.h>
#include <Processors/Chunk.h>

#include <memory>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

class MergeTreeData;

/// Streaming writer that consumes a sequence of parts in chunks, appending their vectors to a
/// DiskANN `.fbin` file while simultaneously populating a `PartRowIdMapWriter` and an
/// `ANNGroupCoverage`. The three outputs are produced in a single pass over the source parts so
/// that they are guaranteed to be consistent with one another.
///
/// Key invariant (the correctness foundation of the whole ANN index chain):
///   the `i`-th row appended to the `.fbin`
///     ⇔ `id_map.records[i] == PartRowId{partition_hash, _block_number, _block_offset}`
///     ⇔ DiskANN's internal `vertex_id` after `build` equals `i`.
///
/// The public API is intentionally a per-chunk step sequence — not a single `dump(parts)` call —
/// so that a long-running build task can yield control between chunks and release its executor
/// slot. For callers that want a one-shot dump there is `dumpFromParts` below, which drives the
/// step loop internally.
///
/// Lifecycle:
///   ctor →
///     (openPart → readNextChunk → writeChunk → readNextChunk → ... → closePart)*
///     → finalizeFbin → move-out outputs.
class VectorStreamWriter
{
public:
    struct Params
    {
        /// Vector column name in the storage schema (must be of type `Array(Float32)`).
        String vector_column_name;

        /// Expected dimensionality. Each row is validated against this value; a mismatch aborts
        /// the writer with `BAD_ARGUMENTS`.
        UInt32 expected_dim = 0;

        /// Seed used by sipHash64 to derive `partition_hash` from `part.info.partition_id`.
        UInt64 hash_seed = 0;

        /// Storage that owns the parts. Required for constructing the sequential source.
        const MergeTreeData * storage = nullptr;

        /// Metadata snapshot used to resolve column names and types when opening the sequential
        /// source.
        StorageSnapshotPtr storage_snapshot;
    };

    VectorStreamWriter(Params params_, DiskANNFbinWriter & fbin_writer_, LoggerPtr log_);
    ~VectorStreamWriter();

    VectorStreamWriter(const VectorStreamWriter &) = delete;
    VectorStreamWriter & operator=(const VectorStreamWriter &) = delete;

    /// Open a new part for reading. Calls `coverage.addPart(...)` synchronously. After this
    /// `readNextChunk` will begin returning chunks from this part until it is exhausted.
    /// Must be paired with `closePart`. A second `openPart` before `closePart` throws.
    void openPart(DataPartPtr part);

    /// Pull the next chunk from the currently open part's pipeline. Returns an empty chunk
    /// (`chunk.hasRows() == false`) when the current part is exhausted.
    Chunk readNextChunk();

    /// Consume one chunk by appending its vectors to the `.fbin` writer and its
    /// `(partition_hash, _block_number, _block_offset)` triples to the id_map writer.
    /// The chunk must come from the currently open part.
    void writeChunk(const Chunk & chunk);

    /// Close the currently open part. Safe to call multiple times.
    void closePart();

    /// Patch the `.fbin` header with the final row count. Must be called before `getWrittenRows`
    /// is considered stable. Idempotent.
    void finalizeFbin();

    /// Drive the complete openPart / readNextChunk / writeChunk / closePart loop for every part
    /// in `parts`, then `finalizeFbin`. Provided as a convenience for callers that do not need
    /// fine-grained yield points (e.g. unit tests and single-shot builders).
    void dumpFromParts(const std::vector<DataPartPtr> & parts);

    /// Accessors for the produced artefacts. After `finalizeFbin`, callers typically
    /// `std::move` the id_map writer / coverage out of the writer into the on-disk
    /// artefacts.
    PartRowIdMapWriter & idMapWriter() { return id_map_writer; }
    ANNGroupCoverage & coverage() { return coverage_; }
    UInt64 getWrittenRows() const { return id_map_writer.size(); }

private:
    void ensureVectorColumnType() const;

    Params params;
    DiskANNFbinWriter & fbin_writer;
    LoggerPtr log;

    PartRowIdMapWriter id_map_writer;
    ANNGroupCoverage coverage_;

    /// Per-part streaming state. Declared here (opaque) so the header can avoid leaking
    /// `QueryPipeline` / `PullingPipelineExecutor` includes to all translation units.
    struct OpenPart;
    std::unique_ptr<OpenPart> current;

    bool finalized = false;
};

}
