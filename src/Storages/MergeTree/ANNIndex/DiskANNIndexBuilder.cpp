#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/DiskANNIndexBuilder.h>

#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>
#include <Storages/MergeTree/DiskANNIndex.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <base/hex.h>

#include <filesystem>
#include <sstream>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace fs = std::filesystem;

namespace
{
    constexpr std::string_view FBIN_FILE_NAME = "vectors.fbin";

    constexpr size_t FBIN_WRITE_BUFFER_SIZE = 1 << 20;          /// 1 MiB
    constexpr size_t META_WRITE_BUFFER_SIZE = 4096;

    std::string formatHexU64(UInt64 v)
    {
        return "0x" + getHexUIntLowercase(v);
    }

    void writeGroupMetaJsonToStorage(
        IANNGroupStorage & storage,
        const ANNIndexDefinition & definition,
        UInt64 hash_seed,
        UInt64 num_points,
        UInt64 build_time_ms)
    {
        Poco::JSON::Object root;
        root.set("version", static_cast<UInt32>(1));

        Poco::JSON::Object shape_obj;
        shape_obj.set("dim", definition.shape.dim);
        shape_obj.set("metric", static_cast<UInt32>(definition.shape.metric));
        shape_obj.set("algorithm", definition.shape.algorithm);
        shape_obj.set("params_hash", formatHexU64(definition.shape.params_hash));
        root.set("shape", shape_obj);

        root.set("hash_algo", definition.hash_algo);
        root.set("hash_seed", formatHexU64(hash_seed));
        root.set("num_points", num_points);
        root.set("build_time_ms", build_time_ms);

        Poco::JSON::Object build_opts;
        build_opts.set("pruned_degree", definition.build_options.pruned_degree);
        build_opts.set("max_degree", definition.build_options.max_degree);
        build_opts.set("l_build", definition.build_options.l_build);
        build_opts.set("alpha", definition.build_options.alpha);
        build_opts.set("num_threads", definition.build_options.num_threads);
        build_opts.set("pq_chunks", definition.build_options.pq_chunks);
        build_opts.set("build_ram_limit_gb", definition.build_options.build_ram_limit_gb);
        root.set("build_options", build_opts);

        const auto * disk_search = dynamic_cast<const DiskANNSearchDefaults *>(definition.search_defaults.get());
        if (!disk_search)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "DiskANNIndexBuilder: `search_defaults` is not a `DiskANNSearchDefaults`");

        Poco::JSON::Object search_defaults;
        search_defaults.set("num_threads", disk_search->options.num_threads);
        search_defaults.set("search_io_limit", disk_search->options.search_io_limit);
        search_defaults.set("num_nodes_to_cache", disk_search->options.num_nodes_to_cache);
        search_defaults.set("default_search_list_size", disk_search->options.default_search_list_size);
        search_defaults.set("default_beam_width", disk_search->options.default_beam_width);
        root.set("search_defaults", search_defaults);

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(root, oss, 2, -1,
            Poco::JSON_WRAP_STRINGS | Poco::JSON_ESCAPE_UNICODE);
        const std::string payload = oss.str();

        auto out = storage.writeFile(std::string(ANNIndexGroup::META_FILE_NAME),
            META_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        out->write(payload.data(), payload.size());
        out->finalize();
    }

    UInt64 totalRowsOf(const std::vector<DataPartPtr> & parts)
    {
        UInt64 total = 0;
        for (const auto & p : parts)
        {
            if (p)
                total += p->rows_count;
        }
        return total;
    }
}


const std::array<DiskANNIndexBuilder::SubtaskFn, 3> DiskANNIndexBuilder::transform_data_subtasks = {
    &DiskANNIndexBuilder::prepareStreamWriter,
    &DiskANNIndexBuilder::transformChunk,
    &DiskANNIndexBuilder::finalizeFbin,
};

const std::array<DiskANNIndexBuilder::SubtaskFn, 3> DiskANNIndexBuilder::build_index_subtasks = {
    &DiskANNIndexBuilder::invokeFfiBuild,
    &DiskANNIndexBuilder::writeSidecarsAndMeta,
    &DiskANNIndexBuilder::sanityReloadGroup,
};


DiskANNIndexBuilder::DiskANNIndexBuilder(
    ANNBuildSelectedEntryPtr entry_,
    ANNIndexManager & manager_,
    const std::string & tmp_dir_)
    : entry(std::move(entry_))
    , manager(manager_)
    , log(getLogger("DiskANNIndexBuilder"))
{
    if (!entry)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANNIndexBuilder: entry must not be null");
    if (entry->selected_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANNIndexBuilder: selected_parts must not be empty");
    if (!entry->storage_snapshot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANNIndexBuilder: storage_snapshot must not be null");
    if (entry->definition.shape.algorithm != "diskann")
    {
        /// Defense-in-depth: the factory function already dispatches by algorithm, but the
        /// builder must not blindly trust its input if the dispatch layer regresses.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANNIndexBuilder: unexpected shape.algorithm `{}`",
            entry->definition.shape.algorithm);
    }
    if (entry->definition.shape.dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANNIndexBuilder: shape.dim must be > 0");
    if (entry->definition.vector_column_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANNIndexBuilder: vector_column_name must not be empty");

    /// Eagerly open the tmp directory so that downstream writes land inside it from the first
    /// `execute()` call. `tmp_dir_` is supplied by the outer task (via `BuildReservation::tmpDir()`)
    /// — by the time we get here the manager already has a registration for `tmp_dir_` in its
    /// `in_flight_builds` set, so the cleanup pass will skip the directory we are about to create.
    tmp_group_storage = manager.createGroupStorage(tmp_dir_);
    if (!tmp_group_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANNIndexBuilder: manager returned null tmp storage");

    stopwatch.restart();
    LOG_DEBUG(log,
        "DiskANN build prepare: parts={}, rows={}, tmp_dir={}",
        entry->selected_parts.size(),
        totalRowsOf(entry->selected_parts),
        tmp_group_storage->getGroupDir());
}

DiskANNIndexBuilder::~DiskANNIndexBuilder() = default;


template <size_t num_subtasks>
bool DiskANNIndexBuilder::stepStage(const std::array<SubtaskFn, num_subtasks> & subtasks, size_t & idx, Stage next_stage)
{
    chassert(idx < num_subtasks);
    const bool subtask_wants_rerun = (this->*subtasks[idx])();
    if (subtask_wants_rerun)
        return true;
    if (++idx == num_subtasks)
    {
        stage = next_stage;
        if (next_stage == Stage::DONE)
            return false;
    }
    return true;
}


bool DiskANNIndexBuilder::execute()
{
    switch (stage)
    {
        case Stage::TRANSFORM_DATA:
            return stepStage(transform_data_subtasks, transform_data_idx, Stage::BUILD_INDEX);
        case Stage::BUILD_INDEX:
            return stepStage(build_index_subtasks, build_index_idx, Stage::DONE);
        case Stage::DONE:
            return false;
    }
    return false;
}


ANNIndexGroupPtr DiskANNIndexBuilder::getResult()
{
    if (stage != Stage::DONE)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANNIndexBuilder::getResult called before execute finished");
    if (!new_group)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANNIndexBuilder::getResult: new_group is null after execute finished");
    return new_group;
}


/// Stage TRANSFORM_DATA --------------------------------------------------------------------

bool DiskANNIndexBuilder::prepareStreamWriter()
{
    /// Writing begins immediately: `DiskLocal` is pass-through so the FFI call in Stage
    /// BUILD_INDEX can read the `.fbin` as soon as it is finalized.
    fbin_buffer = tmp_group_storage->writeFile(
        std::string(FBIN_FILE_NAME),
        FBIN_WRITE_BUFFER_SIZE,
        WriteMode::Rewrite,
        WriteSettings{});

    fbin_writer = std::make_unique<DiskANNFbinWriter>(*fbin_buffer, entry->definition.shape.dim);

    VectorStreamWriter::Params params;
    params.vector_column_name = entry->definition.vector_column_name;
    params.expected_dim = entry->definition.shape.dim;
    params.hash_seed = manager.getHashSeed();
    params.storage_snapshot = entry->storage_snapshot;
    /// The `MergeTreeData *` is recovered from `storage_snapshot` — see below.
    params.storage = dynamic_cast<const MergeTreeData *>(&entry->storage_snapshot->storage);
    if (!params.storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANNIndexBuilder: storage_snapshot->storage is not a MergeTreeData");

    vector_stream_writer = std::make_unique<VectorStreamWriter>(std::move(params), *fbin_writer, log);

    current_part_idx = 0;
    current_part_open = false;
    return false;
}


bool DiskANNIndexBuilder::transformChunk()
{
    if (!current_part_open)
    {
        if (current_part_idx == entry->selected_parts.size())
        {
            /// All parts drained — advance to the next subtask.
            return false;
        }
        const auto & part = entry->selected_parts[current_part_idx];
        if (!part)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "DiskANNIndexBuilder: null part at index {}", current_part_idx);

        vector_stream_writer->openPart(part);
        current_part_open = true;
        /// Yield after opening: coverage is recorded; the next invocation pulls the first chunk.
        return true;
    }

    Chunk chunk = vector_stream_writer->readNextChunk();
    if (chunk.hasRows())
    {
        vector_stream_writer->writeChunk(chunk);
        /// Yield between chunks.
        return true;
    }

    /// Pipeline drained — close the part and advance the cursor.
    vector_stream_writer->closePart();
    current_part_open = false;
    ++current_part_idx;
    return true;
}


bool DiskANNIndexBuilder::finalizeFbin()
{
    /// `VectorStreamWriter::finalizeFbin` patches the header count through `DiskANNFbinWriter`;
    /// the underlying `fbin_buffer` still owns the file descriptor and must be finalized +
    /// closed explicitly so that the data is flushed before the FFI reads it.
    vector_stream_writer->finalizeFbin();
    if (fbin_buffer)
    {
        fbin_buffer->finalize();
        fbin_buffer.reset();
    }
    fbin_writer.reset();

    const UInt64 written_rows = vector_stream_writer->getWrittenRows();
    if (written_rows == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build: wrote zero rows to vectors.fbin — selected_parts had {} parts with total {} rows",
            entry->selected_parts.size(), totalRowsOf(entry->selected_parts));
    if (written_rows != vector_stream_writer->idMapWriter().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build: fbin row count ({}) does not match id_map size ({})",
            written_rows, vector_stream_writer->idMapWriter().size());

    return false;
}


/// Stage BUILD_INDEX -----------------------------------------------------------------------

bool DiskANNIndexBuilder::invokeFfiBuild()
{
    const auto group_full_path = tmp_group_storage->getFullPath();
    const auto fbin_path = (fs::path(group_full_path) / FBIN_FILE_NAME).string();
    const auto index_prefix = (fs::path(group_full_path) / DiskANNArtifactNames::INDEX_PREFIX_BASENAME).string();

    LOG_DEBUG(log, "DiskANN build FFI: data_path={}, index_prefix={}", fbin_path, index_prefix);

    DiskANNDiskIndexBuilder ffi_builder(
        entry->definition.shape.dim,
        static_cast<DiskANNMetric>(entry->definition.shape.metric),
        entry->definition.build_options);
    ffi_builder.setDataPath(fbin_path);
    ffi_builder.setIndexPrefix(index_prefix);
    ffi_builder.build();
    return false;
}


bool DiskANNIndexBuilder::writeSidecarsAndMeta()
{
    /// Drop the uncompressed `vectors.fbin`; the FFI has consumed it.
    tmp_group_storage->removeFileIfExists(std::string(FBIN_FILE_NAME));

    /// Persist the sidecars and the per-group `meta.json` alongside the index prefix.
    vector_stream_writer->idMapWriter().writeTo(*tmp_group_storage, WriteSettings{});
    vector_stream_writer->coverage().writeTo(*tmp_group_storage);
    writeGroupMetaJsonToStorage(
        *tmp_group_storage,
        entry->definition,
        manager.getHashSeed(),
        vector_stream_writer->getWrittenRows(),
        stopwatch.elapsedMilliseconds());
    return false;
}


bool DiskANNIndexBuilder::sanityReloadGroup()
{
    /// Re-open the freshly-written artefacts. This verifies self-consistency of
    /// `meta.json` vs `id_map.bin` vs `coverage.bin` before the group becomes observable.
    new_group = ANNIndexGroup::load(tmp_group_storage, entry->definition.search_defaults);

    const UInt64 expected_rows = totalRowsOf(entry->selected_parts);
    if (new_group->numPoints() != expected_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build sanity: new_group->numPoints()={} does not match expected rows={}",
            new_group->numPoints(), expected_rows);
    if (!(new_group->getShape() == manager.getShape()))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build sanity: new_group shape mismatch with manager shape");
    if (new_group->getHashSeed() != manager.getHashSeed())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build sanity: new_group hash_seed mismatch with manager hash_seed");

    LOG_INFO(log,
        "DiskANN build artefacts ready: points={}, wall_ms={}",
        new_group->numPoints(),
        stopwatch.elapsedMilliseconds());
    return false;
}


/// Factory dispatch ------------------------------------------------------------------------

std::unique_ptr<IANNIndexBuilder> createANNIndexBuilder(
    ANNBuildSelectedEntryPtr entry, ANNIndexManager & manager, const std::string & tmp_dir)
{
    if (!entry)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "createANNIndexBuilder: entry must not be null");

    const auto & algo = entry->definition.shape.algorithm;
    if (algo == "diskann")
        return std::make_unique<DiskANNIndexBuilder>(std::move(entry), manager, tmp_dir);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "Unknown ANN index algorithm: `{}`", algo);
}

}

#endif
