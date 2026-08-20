#include <Storages/StorageMergeTreeParts.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/BorrowedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
}

class StorageMergeTreePartsSource : public ISource, WithContext
{
public:
    struct PartsToReadInfo;
    using PartsToReadInfoPtr = std::shared_ptr<PartsToReadInfo>;
    using ReadFromPartsInfo = StorageMergeTreeParts::ReadFromPartsInfo;
    using ReadFromPart = StorageMergeTreeParts::ReadFromPartsInfo::ReadFromPart;

    struct PartsToReadInfo
    {
        ReadFromPartsInfo read_from_parts_info;
        std::atomic<size_t> next_file_to_read = 0;

        explicit PartsToReadInfo(const ReadFromPartsInfo & read_from_parts_info_) : read_from_parts_info(read_from_parts_info_) {}

        std::optional<ReadFromPart> getNext()
        {
            auto current_part_idx = next_file_to_read.fetch_add(1);

            if (current_part_idx >= read_from_parts_info.parts.size())
                return std::nullopt;

            return read_from_parts_info.parts[current_part_idx];
        }
    };

    StorageMergeTreePartsSource(
        PartsToReadInfoPtr read_info_,
        SharedHeader header_,
        StorageMetadataPtr storage_metadata_,
        const StorageSnapshotPtr & storage_snapshot_,
        FilterDAGInfoPtr row_policy_filter_,
        PrewhereInfoPtr prewhere_info_,
        size_t max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , requested_columns(header_->getNamesAndTypesList())
        , storage_columns(storage_metadata_->getColumns().getAllPhysical())
        , column_names(header_->getNames())
        , storage_metadata(storage_metadata_)
        , storage_snapshot(storage_snapshot_)
        , reader_settings(MergeTreeReaderSettings::createFromContext(context_))
        , row_policy_filter(row_policy_filter_)
        , prewhere_info(prewhere_info_)
        , max_block_size(max_block_size_)
        , read_info(read_info_)
    {
        auto other_prewhere_actions = MergeTreeSelectProcessor::getPrewhereActions(
            row_policy_filter,
            prewhere_info,
            IndexReadTasks{},
            ExpressionActionsSettings(context_),
            reader_settings.enable_multiple_prewhere_read_steps,
            reader_settings.force_short_circuit_execution);

        prewhere_actions.steps.insert(prewhere_actions.steps.end(), other_prewhere_actions.steps.begin(), other_prewhere_actions.steps.end());
    }

    String getName() const override { return "MergeTreeParts"; }

    Chunk generate() override
    {
        auto component_guard = Coordination::setCurrentComponent("StorageMergeTreePartsSource::generate");

        while (true)
        {
            /// A selective PREWHERE can keep this loop spinning over many fully-filtered batches, poll for cancel.
            if (isCancelled())
                return {};

            if (!state)
            {
                auto next_part = read_info->getNext();
                if (!next_part)
                    return {};

                initializeReaderState(*next_part);
            }

            std::vector<MarkRanges> patch_ranges;
            auto read_result = state->readers_chain.read(max_block_size, state->part.ranges, patch_ranges);

            LOG_TEST(log, "Having {} rows", read_result.num_rows);

            /// Report rows scanned before PREWHERE filtering, also when the whole block was filtered out.
            if (read_result.numReadRows() || read_result.numBytesRead())
                progress(read_result.numReadRows(), read_result.numBytesRead());

            if (read_result.num_rows != 0)
            {
                /// The read result may contain more columns than the output header (e.g. prewhere
                /// filter columns that were kept for filtering but not needed afterwards).
                /// Mirror MergeTreeReadTask::read + MergeTreeSelectProcessor::readCurrentTask:
                /// build a named block from the chain's sample block, then pick only the header columns.
                const auto & sample_block = state->readers_chain.getSampleBlock();
                Block block = sample_block.cloneWithColumns(read_result.columns);

                const auto & header = getPort().getHeader();
                Columns output_columns;
                output_columns.reserve(header.columns());
                for (size_t i = 0; i < header.columns(); ++i)
                    output_columns.push_back(block.getByName(header.getByPosition(i).name).column);

                return Chunk(std::move(output_columns), read_result.num_rows);
            }
            /// 0 rows means the batch was fully filtered by PREWHERE - not end of the part.
            if (state->part.ranges.empty() && state->readers_chain.isCurrentRangeFinished())
                state.reset();
        }
    }

    MergeTreeReadTask::Readers createReaders(
        MergeTreeDataPartInfoForReaderPtr info_for_reader,
        const MergeTreeReadTaskColumns & task_columns,
        const MarkRanges & mark_ranges,
        const MergeTreeSettingsPtr & storage_settings) const
    {
        auto get_reader = [&](const NamesAndTypesList & columns_to_read)
        {
            return createMergeTreeReader(
                info_for_reader,
                columns_to_read,
                storage_snapshot,
                storage_settings,
                mark_ranges,
                VirtualFields{},
                getContext()->getUncompressedCache().get(),
                getContext()->getMarkCache().get(),
                nullptr,
                reader_settings,
                ValueSizeMap{},
                ReadBufferFromFileBase::ProfileCallback{});
        };

        MergeTreeReadTask::Readers readers;
        readers.main = get_reader(task_columns.columns);

        for (const auto & pre_columns_per_step : task_columns.pre_columns)
            readers.prewhere.push_back(get_reader(pre_columns_per_step));

        return readers;
    }

    void initializeReaderState(const ReadFromPart & part)
    {
        state = std::make_unique<ReaderState>(part);
        const auto & read_from_parts_info = read_info->read_from_parts_info;

        /// store/707/70794cd7-9505-4011-9400-fde425bb25d1/20000101_1_1_0
        fs::path part_relative_path = state->part.relative_path;
        if (state->part.relative_path.ends_with("/"))
            part_relative_path = part_relative_path.parent_path(); /// Get rid of trailing "/"
        /// 20000101_1_1_0
        std::string part_name = part_relative_path.filename();
        /// store/707/70794cd7-9505-4011-9400-fde425bb25d1
        std::string part_relative_root_path = part_relative_path.parent_path();

        LOG_DEBUG(log, "Initializing reader for part `{}` (`{}`) with ranges: {}", part_name, part.relative_path, toString(part.ranges));

        auto all_columns = storage_columns;
        if (state->part.has_lightweight_delete)
            all_columns.emplace_back(RowExistsColumn::name, RowExistsColumn::type);

        auto disk = read_from_parts_info.disk;
        auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);

        auto [data_part_storage, mark_type] = MergeTreeDataPartBuilder::getPartStorageAndMarkType(single_disk_volume, part_relative_root_path, part_name, getReadSettings());
        if (!data_part_storage || !mark_type)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot determine part storage type and mark type from disk");

        auto metadata_read_settings = getContext()->getReadSettings().adjustBufferSize(4096);

        /// The part's physical columns (columns.txt), used as the reader's column view so columns
        /// absent from the part fill from defaults. Fresh list - readText appends (seeding would dup).
        NamesAndTypesList serialization_columns;
        if (auto columns_buf = data_part_storage->readFileIfExists("columns.txt", metadata_read_settings, {}))
            serialization_columns.readText(*columns_buf);
        else
            serialization_columns = all_columns;

        /// SerializationInfo::Settings is used as default because ratio_of_defaults_for_sparse does not affect read operations.
        SerializationInfoByName serialization_infos({});
        if (data_part_storage->existsFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME))
            serialization_infos = SerializationInfoByName::readJSON(
                serialization_columns, *data_part_storage->readFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, metadata_read_settings, {}));

        MergeTreeIndexGranularityInfo index_granularity_info(
            *mark_type,
            /* fixed_index_granularity */0,
            /* index_granularity_bytes */read_from_parts_info.index_granularity_bytes);

        MergeTreeIndexGranularityPtr index_granularity;
        MergeTreeSettingsPtr storage_settings = std::make_shared<MergeTreeSettings>();

        /// Must be read before the switch: Compact parts need getTotalSubstreams() to
        /// compute marks_per_granule correctly when mark_type.with_substreams is true.
        ColumnsSubstreams columns_substreams;
        auto columns_substreams_buf = data_part_storage->readFileIfExists("columns_substreams.txt", metadata_read_settings, {});
        if (columns_substreams_buf)
            columns_substreams.readText(*columns_substreams_buf);

        switch (part.type.getValue())
        {
            case MergeTreeDataPartType::Wide:
            {
                index_granularity_info.changeGranularityIfRequired(*data_part_storage);

                String filename;
                /// Granularity is shared across a part's columns; load it from any present column.
                /// A requested subcolumn (e.g. a.size0) arrives flat with no stream file - use its parent.
                /// `requested_columns` can be empty (e.g. `SELECT count() ... PREWHERE ...`, whose header is
                /// emptied once the prewhere column is removed), so fall back to any present part column.
                if (serialization_columns.empty())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "StorageMergeTreeParts found no columns in wide part {} with path {}",
                                    part_name, part_relative_root_path);
                NameAndTypePair column = requested_columns.empty()
                    ? serialization_columns.front()
                    : requested_columns.front();
                if (!serialization_columns.contains(column.name))
                {
                    String storage_name = column.name;
                    if (auto resolved = storage_metadata->getColumns().tryGetColumnOrSubcolumn(
                            GetColumnsOptions::AllPhysical, column.name))
                        storage_name = resolved->getNameInStorage();

                    if (auto storage_column = serialization_columns.tryGetByName(storage_name))
                        column = *storage_column;
                    else if (!serialization_columns.empty())
                        column = serialization_columns.front();
                }
                auto it = serialization_infos.find(column.getNameInStorage());
                auto serialization = it == serialization_infos.end()
                    ? IDataType::getSerialization(column)
                    : IDataType::getSerialization(column, *it->second);

                serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
                {
                    if (filename.empty())
                    {
                        /// Use the storage-based overload so that hash-based stream names
                        /// (written when column names exceed the hash threshold) are resolved
                        /// by checking which .bin file actually exists on disk.
                        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                            column, substream_path,
                            IMergeTreeDataPart::DATA_FILE_EXTENSION,
                            *data_part_storage, storage_settings);
                        if (stream_name)
                            filename = *stream_name;
                        else
                            throw Exception(ErrorCodes::CORRUPTED_DATA, "StorageMergeTreeParts could not get filename for column {} in part {} with path {}",
                                            column.name, part_name, part_relative_root_path);
                    }
                });

                index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>();
                MergeTreeDataPartWide::loadIndexGranularityImpl(index_granularity, index_granularity_info, *data_part_storage, filename, *storage_settings);
                break;
            }
            case MergeTreeDataPartType::Compact:
            {
                /// When with_substreams is true the mark file stores one entry per substream
                /// (not per column), so we must pass the total substream count rather than the
                /// column count.  Passing the wrong value misaligns every granularity read.
                size_t marks_per_granule = index_granularity_info.mark_type.with_substreams
                    ? columns_substreams.getTotalSubstreams()
                    : serialization_columns.size();
                if (!marks_per_granule)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "StorageMergeTreeParts found zero marks per granule in part {} with path {}",
                                    part_name, part_relative_root_path);
                index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>();
                MergeTreeDataPartCompact::loadIndexGranularityImpl(index_granularity, index_granularity_info, marks_per_granule, *data_part_storage, *storage_settings);
                break;
            }
            case MergeTreeDataPartType::Unknown:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Data parts of type `{}` are not allowed", part.type.toString());
            }
        }

        MergeTreeDataPartChecksums checksums;
        auto buf = data_part_storage->readFile("checksums.txt", metadata_read_settings, {});
        checksums.read(*buf);

        auto table_name = storage_snapshot->storage.getStorageID().getNameForLogs();
        auto info_for_reader = std::make_shared<BorrowedMergeTreeDataPartInfoForReader>(
            part.type, data_part_storage, serialization_columns, std::move(columns_substreams), index_granularity_info, index_granularity,
            checksums, serialization_infos, storage_settings, table_name, part.marks_count, getContext());

        auto actions_settings = ExpressionActionsSettings(getContext());

        /// Build the lightweight delete mutation step for this part if needed.
        /// This mirrors MergeTreeReadPoolBase: the step is per-part because different
        /// parts may or may not have lightweight deletes.
        PrewhereExprSteps part_mutation_steps;
        if (reader_settings.apply_deleted_mask && state->part.has_lightweight_delete)
        {
            bool remove_filter_column = std::ranges::find(column_names, RowExistsColumn::name) == column_names.end();
            part_mutation_steps.push_back(createLightweightDeleteStep(remove_filter_column));
        }

        state->task_columns = getReadTaskColumns(
            *info_for_reader,
            storage_snapshot,
            column_names,
            row_policy_filter,
            prewhere_info,
            part_mutation_steps,
            /*index_read_tasks=*/ {},
            actions_settings,
            reader_settings,
            /// MergeTree parts support subcolumns (read from the parent column's streams); resolve
            /// them like the regular read pool, which also passes true.
            /*with_subcolumns=*/ true);

        state->readers = createReaders(info_for_reader, state->task_columns, part.ranges, storage_settings);

        /// Combine per-part mutation steps with the shared prewhere actions, matching
        /// the order expected by MergeTreeReadTask::createReadersChain (mutation steps first).
        /// The readers of the chain keep raw pointers to the steps, so the combined list is kept
        /// in the state and outlives the chain.
        for (const auto & step : part_mutation_steps)
            state->prewhere_actions.steps.push_back(step);
        for (const auto & step : prewhere_actions.steps)
            state->prewhere_actions.steps.push_back(step);

        state->readers_chain = MergeTreeReadTask::createReadersChain(
            state->readers, state->prewhere_actions, read_steps_performance_counters,
            reader_settings.collect_predicate_statistics);

        LOG_DEBUG(log, "Initialized reader for part `{}` (`{}`) with ranges: {}", part_name, part.relative_path, toString(part.ranges));
    }

private:
    NamesAndTypesList requested_columns;
    NamesAndTypesList storage_columns;
    Names column_names;
    StorageMetadataPtr storage_metadata;
    StorageSnapshotPtr storage_snapshot;

    MergeTreeReaderSettings reader_settings; /// Do we need to override some default settings?

    FilterDAGInfoPtr row_policy_filter;
    PrewhereInfoPtr prewhere_info;
    PrewhereExprInfo prewhere_actions;

    size_t max_block_size;
    LoggerPtr log = getLogger("StorageMergeTreePartsSource");

    PartsToReadInfoPtr read_info;

    struct ReaderState
    {
        explicit ReaderState(const ReadFromPart & part_) : part(part_) {}

        MergeTreeReadTask::Readers readers;
        /// Must outlive `readers_chain`, whose readers hold raw pointers to its steps.
        PrewhereExprInfo prewhere_actions;
        MergeTreeReadersChain readers_chain;
        ReadFromPart part;
        MergeTreeReadTaskColumns task_columns;
    };

    using ReaderStatePtr = std::unique_ptr<ReaderState>;

    ReaderStatePtr state;
    ReadStepsPerformanceCounters read_steps_performance_counters;
};

VirtualColumnsDescription StorageMergeTreeParts::createVirtuals()
{
    VirtualColumnsDescription desc;
    /// Register the same persistent virtual columns as MergeTreeData so that
    /// StorageSnapshot::check accepts them when the query explicitly selects e.g.
    /// _block_number or _block_offset.  These columns are physically stored in
    /// parts (enable_block_number_column / enable_block_offset_column) and live in
    /// VirtualColumnsDescription rather than in ColumnsDescription.
    desc.addPersistent(
        RowExistsColumn::name, RowExistsColumn::type, nullptr,
        "Persisted mask created by lightweight delete that show whether row exists or is deleted");
    desc.addPersistent(
        BlockNumberColumn::name, BlockNumberColumn::type, BlockNumberColumn::codec,
        "Persisted original number of block that was assigned at insert");
    desc.addPersistent(
        BlockOffsetColumn::name, BlockOffsetColumn::type, BlockOffsetColumn::codec,
        "Persisted original number of row in block that was assigned at insert");
    return desc;
}

StorageMergeTreeParts::StorageMergeTreeParts(
    const ReadFromPartsInfo & read_from_parts_info_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , read_from_parts_info(read_from_parts_info_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageMergeTreeParts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    /// At least one source, so that the pipe has a header even when there is nothing to read.
    num_streams = std::max<size_t>(1, std::min(num_streams, read_from_parts_info.parts.size()));

    Pipes pipes;
    pipes.reserve(num_streams);

    auto parts_info = std::make_shared<StorageMergeTreePartsSource::PartsToReadInfo>(read_from_parts_info);
    /// Apply the prewhere transformation so that the pipe header matches what the query plan expects:
    /// `MergeTreeSelectProcessor::transformHeader` removes the columns that are only used for prewhere
    /// filtering and are not needed afterwards.
    auto header = std::make_shared<const Block>(
        MergeTreeSelectProcessor::transformHeader(
            storage_snapshot->getSampleBlockForColumns(column_names),
            query_info.row_level_filter,
            query_info.prewhere_info));

    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(
            std::make_shared<StorageMergeTreePartsSource>(
                parts_info, header, storage_snapshot->metadata, storage_snapshot, query_info.row_level_filter, query_info.prewhere_info, max_block_size, context_));
    }

    return Pipe::unitePipes(std::move(pipes));
}

}
