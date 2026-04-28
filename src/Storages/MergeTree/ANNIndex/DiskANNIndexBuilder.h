#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/DiskANNFbinWriter.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Storages/MergeTree/ANNIndex/IANNIndexBuilder.h>
#include <Storages/MergeTree/ANNIndex/VectorStreamWriter.h>

#include <Common/Logger.h>
#include <Common/Stopwatch.h>

#include <IO/WriteBufferFromFileBase.h>

#include <array>
#include <cstddef>
#include <memory>


namespace DB
{

class ANNIndexManager;


/// DiskANN implementation of `IANNIndexBuilder`. Consumes a batch of parts and produces a
/// fully-populated `tmp_ann_<uuid>/` directory plus a loaded `ANNIndexGroup`.
///
/// Internally structured as a two-level state machine (stages × subtasks), modelled on
/// `MergeTask`:
///
///   Stage TRANSFORM_DATA (3 subtasks)
///     - `prepareStreamWriter`  — open the `.fbin` writer and the `VectorStreamWriter` once.
///     - `transformChunk`       — single part-chunk yield point. On each call it either opens
///                                the next part, pulls one chunk, or closes the exhausted part.
///     - `finalizeFbin`         — patch the fbin header count; sanity-check row counts.
///
///   Stage BUILD_INDEX (3 subtasks)
///     - `invokeFfiBuild`        — long, unyieldable DiskANN FFI call.
///     - `writeSidecarsAndMeta`  — drop the fbin, persist `id_map.bin` / `coverage.bin` and
///                                 the per-group `meta.json`.
///     - `sanityReloadGroup`     — reopen the freshly-written artefacts via
///                                 `ANNIndexGroup::load` as a consistency check, then stash
///                                 the resulting group in `new_group`.
///
/// The class owns the fbin writer, stream writer, and Stopwatch that live across stages; the
/// `tmp_group_storage` pointer is moved out of the builder into the returned group at the very
/// end, so that the outer task can rename the underlying directory safely.
class DiskANNIndexBuilder final : public IANNIndexBuilder
{
public:
    DiskANNIndexBuilder(
        ANNBuildSelectedEntryPtr entry_,
        ANNIndexManager & manager_,
        const std::string & tmp_dir_);
    ~DiskANNIndexBuilder() override;

    DiskANNIndexBuilder(const DiskANNIndexBuilder &) = delete;
    DiskANNIndexBuilder & operator=(const DiskANNIndexBuilder &) = delete;

    bool execute() override;
    ANNIndexGroupPtr getResult() override;

private:
    enum class Stage : uint8_t
    {
        TRANSFORM_DATA,
        BUILD_INDEX,
        DONE,
    };

    using SubtaskFn = bool (DiskANNIndexBuilder::*)();

    template <size_t num_subtasks>
    bool stepStage(const std::array<SubtaskFn, num_subtasks> & subtasks, size_t & idx, Stage next_stage);

    /// Stage TRANSFORM_DATA.
    bool prepareStreamWriter();
    bool transformChunk();
    bool finalizeFbin();

    /// Stage BUILD_INDEX.
    bool invokeFfiBuild();
    bool writeSidecarsAndMeta();
    bool sanityReloadGroup();

    static const std::array<SubtaskFn, 3> transform_data_subtasks;
    static const std::array<SubtaskFn, 3> build_index_subtasks;

    /// Inputs.
    ANNBuildSelectedEntryPtr entry;
    ANNIndexManager & manager;
    LoggerPtr log;

    /// Machine state.
    Stage stage = Stage::TRANSFORM_DATA;
    size_t transform_data_idx = 0;
    size_t build_index_idx = 0;

    /// Lifecycle artefacts built up across stages.
    ANNGroupStoragePtr tmp_group_storage;
    std::unique_ptr<WriteBufferFromFileBase> fbin_buffer;
    std::unique_ptr<DiskANNFbinWriter> fbin_writer;
    std::unique_ptr<VectorStreamWriter> vector_stream_writer;

    /// Per-chunk progress for Stage TRANSFORM_DATA. `current_part_idx` points at the part being
    /// read; when it equals `selected_parts.size()` the stage is done.
    size_t current_part_idx = 0;
    bool current_part_open = false;

    Stopwatch stopwatch;
    ANNIndexGroupPtr new_group;
};

}

#endif
