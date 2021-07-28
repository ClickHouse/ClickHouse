#include <Storages/MergeTree/MutateTask.h>

#include <common/logger_useful.h>

#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>

#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>

#include <Interpreters/MutationsInterpreter.h>


#include <Processors/Sources/SourceFromSingleChunk.h>


#include <DataStreams/TTLBlockInputStream.h>
#include <DataStreams/DistinctSortedBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataStreams/SquashingBlockInputStream.h>

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric PartMutation;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

namespace MutationHelpers
{


static bool checkOperationIsNotCanceled(ActionBlocker & merges_blocker, MergeListEntry & mutate_entry)
{
    if (merges_blocker.isCancelled() || mutate_entry->is_cancelled)
        throw Exception("Cancelled mutating parts", ErrorCodes::ABORTED);

    return true;
}


/** Split mutation commands into two parts:
*   First part should be executed by mutations interpreter.
*   Other is just simple drop/renames, so they can be executed without interpreter.
*/
static void splitMutationCommands(
    MergeTreeData::DataPartPtr part,
    const MutationCommands & commands,
    MutationCommands & for_interpreter,
    MutationCommands & for_file_renames)
{
    ColumnsDescription part_columns(part->getColumns());

    if (!isWidePart(part))
    {
        NameSet mutated_columns;
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
                for (const auto & [column_name, expr] : command.column_to_update_expression)
                    mutated_columns.emplace(column_name);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX || command.type == MutationCommand::Type::DROP_PROJECTION)
            {
                for_file_renames.push_back(command);
            }
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::DROP_COLUMN)
                {
                    mutated_columns.emplace(command.column_name);
                }
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    for_interpreter.push_back(
                    {
                        .type = MutationCommand::Type::READ_COLUMN,
                        .column_name = command.rename_to,
                    });
                    mutated_columns.emplace(command.column_name);
                    part_columns.rename(command.column_name, command.rename_to);
                }
            }
        }
        /// If it's compact part, then we don't need to actually remove files
        /// from disk we just don't read dropped columns
        for (const auto & column : part->getColumns())
        {
            if (!mutated_columns.count(column.name))
                for_interpreter.emplace_back(
                    MutationCommand{.type = MutationCommand::Type::READ_COLUMN, .column_name = column.name, .data_type = column.type});
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX || command.type == MutationCommand::Type::DROP_PROJECTION)
            {
                for_file_renames.push_back(command);
            }
            /// If we don't have this column in source part, than we don't need
            /// to materialize it
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::READ_COLUMN)
                {
                    for_interpreter.push_back(command);
                }
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    part_columns.rename(command.column_name, command.rename_to);
                    for_file_renames.push_back(command);
                }
                else
                {
                    for_file_renames.push_back(command);
                }
            }
        }
    }
}


} // namespace MutationHelpers


bool MutateTask::execute()
{
    auto part = main();
    promise.set_value(part);
    return false;
}

MergeTreeData::MutableDataPartPtr MutateTask::main()
{
    MutationHelpers::checkOperationIsNotCanceled(merges_blocker, mutate_entry);

    if (future_part->parts.size() != 1)
        throw Exception("Trying to mutate " + toString(future_part->parts.size()) + " parts, not one. "
            "This is a bug.", ErrorCodes::LOGICAL_ERROR);

    CurrentMetrics::Increment num_mutations{CurrentMetrics::PartMutation};
    const auto & source_part = future_part->parts[0];
    auto storage_from_source_part = StorageFromMergeTreeDataPart::create(source_part);

    auto context_for_reading = Context::createCopy(context);
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);
    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    context_for_reading->setSetting("force_index_by_date", Field(0));
    context_for_reading->setSetting("force_primary_key", Field(0));

    MutationCommands commands_for_part;
    for (const auto & command : *commands)
    {
        if (command.partition == nullptr || future_part->parts[0]->info.partition_id == data.getPartitionIDFromQuery(
                command.partition, context_for_reading))
            commands_for_part.emplace_back(command);
    }

    if (source_part->isStoredOnDisk() && !isStorageTouchedByMutations(
        storage_from_source_part, metadata_snapshot, commands_for_part, Context::createCopy(context_for_reading)))
    {
        LOG_TRACE(log, "Part {} doesn't change up to mutation version {}", source_part->name, future_part->part_info.mutation);
        return data.cloneAndLoadDataPartOnSameDisk(source_part, "tmp_clone_", future_part->part_info, metadata_snapshot);
    }
    else
    {
        LOG_TRACE(log, "Mutating part {} to mutation version {}", source_part->name, future_part->part_info.mutation);
    }

    BlockInputStreamPtr in = nullptr;
    Block updated_header;
    std::unique_ptr<MutationsInterpreter> interpreter;

    const auto data_settings = data.getSettings();
    MutationCommands for_interpreter;
    MutationCommands for_file_renames;

    MutationHelpers::splitMutationCommands(source_part, commands_for_part, for_interpreter, for_file_renames);

    UInt64 watch_prev_elapsed = 0;
    MergeStageProgress stage_progress(1.0);

    NamesAndTypesList storage_columns = metadata_snapshot->getColumns().getAllPhysical();
    NameSet materialized_indices;
    NameSet materialized_projections;
    MutationsInterpreter::MutationKind::MutationKindEnum mutation_kind
        = MutationsInterpreter::MutationKind::MutationKindEnum::MUTATE_UNKNOWN;

    if (!for_interpreter.empty())
    {
        interpreter = std::make_unique<MutationsInterpreter>(
            storage_from_source_part, metadata_snapshot, for_interpreter, context_for_reading, true);
        materialized_indices = interpreter->grabMaterializedIndices();
        materialized_projections = interpreter->grabMaterializedProjections();
        mutation_kind = interpreter->getMutationKind();
        in = interpreter->execute();
        updated_header = interpreter->getUpdatedHeader();
        in->setProgressCallback(MergeProgressCallback(mutate_entry, watch_prev_elapsed, stage_progress));
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + future_part->name, space_reservation->getDisk(), 0);
    auto new_data_part = data.createPart(
        future_part->name, future_part->type, future_part->part_info, single_disk_volume, "tmp_mut_" + future_part->name);

    new_data_part->uuid = future_part->uuid;
    new_data_part->is_temp = true;
    new_data_part->ttl_infos = source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    new_data_part->index_granularity_info = source_part->index_granularity_info;
    new_data_part->setColumns(MergeTreeDataMergerMutator::getColumnsForNewDataPart(source_part, updated_header, storage_columns, for_file_renames));
    new_data_part->partition.assign(source_part->partition);

    auto disk = new_data_part->volume->getDisk();
    String new_part_tmp_path = new_data_part->getFullRelativePath();

    SyncGuardPtr sync_guard;
    if (data.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(new_part_tmp_path);

    /// Don't change granularity type while mutating subset of columns
    auto mrk_extension = source_part->index_granularity_info.is_adaptive ? getAdaptiveMrkExtension(new_data_part->getType())
                                                                         : getNonAdaptiveMrkExtension();
    bool need_sync = needSyncPart(source_part->rows_count, source_part->getBytesOnDisk(), *data_settings);
    bool need_remove_expired_values = false;

    if (in && MergeTreeDataMergerMutator::shouldExecuteTTL(metadata_snapshot, interpreter->getColumnDependencies(), commands_for_part))
        need_remove_expired_values = true;

    /// All columns from part are changed and may be some more that were missing before in part
    /// TODO We can materialize compact part without copying data
    if (!isWidePart(source_part)
        || (mutation_kind == MutationsInterpreter::MutationKind::MUTATE_OTHER && interpreter && interpreter->isAffectingAllColumns()))
    {
        disk->createDirectories(new_part_tmp_path);

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
        auto compression_codec = data.getCompressionCodecForPart(source_part->getBytesOnDisk(), source_part->ttl_infos, time_of_mutation);

        auto part_indices = MergeTreeDataMergerMutator::getIndicesForNewDataPart(metadata_snapshot->getSecondaryIndices(), for_file_renames);
        auto part_projections = MergeTreeDataMergerMutator::getProjectionsForNewDataPart(metadata_snapshot->getProjections(), for_file_renames);

        mutateAllPartColumns(
            new_data_part,
            // metadata_snapshot,
            part_indices,
            part_projections,
            in,
            // time_of_mutation,
            compression_codec,
            mutate_entry,
            need_remove_expired_values,
            need_sync
            // space_reservation,
            // holder,
            // context
            );

        /// no finalization required, because mutateAllPartColumns use
        /// MergedBlockOutputStream which finilaze all part fields itself
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        /// We will modify only some of the columns. Other columns and key values can be copied as-is.
        NameSet updated_columns;
        if (mutation_kind != MutationsInterpreter::MutationKind::MUTATE_INDEX_PROJECTION)
        {
            for (const auto & name_type : updated_header.getNamesAndTypesList())
                updated_columns.emplace(name_type.name);
        }

        auto indices_to_recalc = MergeTreeDataMergerMutator::getIndicesToRecalculate(
            in, updated_columns, metadata_snapshot, context, materialized_indices, source_part);
        auto projections_to_recalc = MergeTreeDataMergerMutator::getProjectionsToRecalculate(
            updated_columns, metadata_snapshot, materialized_projections, source_part);

        NameSet files_to_skip = MergeTreeDataMergerMutator::collectFilesToSkip(
            source_part,
            mutation_kind == MutationsInterpreter::MutationKind::MUTATE_INDEX_PROJECTION ? Block{} : updated_header,
            indices_to_recalc,
            mrk_extension,
            projections_to_recalc);
        NameToNameVector files_to_rename = MergeTreeDataMergerMutator::collectFilesForRenames(source_part, for_file_renames, mrk_extension);

        if (indices_to_recalc.empty() && projections_to_recalc.empty() && mutation_kind != MutationsInterpreter::MutationKind::MUTATE_OTHER
            && files_to_rename.empty())
        {
            LOG_TRACE(
                log, "Part {} doesn't change up to mutation version {} (optimized)", source_part->name, future_part->part_info.mutation);
            return data.cloneAndLoadDataPartOnSameDisk(source_part, "tmp_clone_", future_part->part_info, metadata_snapshot);
        }

        if (need_remove_expired_values)
            files_to_skip.insert("ttl.txt");

        disk->createDirectories(new_part_tmp_path);

        /// Create hardlinks for unchanged files
        for (auto it = disk->iterateDirectory(source_part->getFullRelativePath()); it->isValid(); it->next())
        {
            if (files_to_skip.count(it->name()))
                continue;

            String destination = new_part_tmp_path;
            String file_name = it->name();
            auto rename_it = std::find_if(files_to_rename.begin(), files_to_rename.end(), [&file_name](const auto & rename_pair) { return rename_pair.first == file_name; });
            if (rename_it != files_to_rename.end())
            {
                if (rename_it->second.empty())
                    continue;
                destination += rename_it->second;
            }
            else
            {
                destination += it->name();
            }

            if (!disk->isDirectory(it->path()))
                disk->createHardLink(it->path(), destination);
            else if (!startsWith("tmp_", it->name())) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                disk->createDirectories(destination);
                for (auto p_it = disk->iterateDirectory(it->path()); p_it->isValid(); p_it->next())
                {
                    String p_destination = destination + "/";
                    String p_file_name = p_it->name();
                    p_destination += p_it->name();
                    disk->createHardLink(p_it->path(), p_destination);
                }
            }
        }

        mutate_entry->columns_written = storage_columns.size() - updated_header.columns();

        new_data_part->checksums = source_part->checksums;

        auto compression_codec = source_part->default_codec;

        if (in)
        {
            mutateSomePartColumns(
                source_part,
                // metadata_snapshot,
                indices_to_recalc,
                projections_to_recalc,
                // If it's an index/projection materialization, we don't write any data columns, thus empty header is used
                mutation_kind == MutationsInterpreter::MutationKind::MUTATE_INDEX_PROJECTION ? Block{} : updated_header,
                new_data_part,
                in,
                // time_of_mutation,
                compression_codec,
                mutate_entry,
                need_remove_expired_values,
                need_sync
                // space_reservation,
                // holder,
                // context
                );
        }

        for (const auto & [rename_from, rename_to] : files_to_rename)
        {
            if (rename_to.empty() && new_data_part->checksums.files.count(rename_from))
            {
                new_data_part->checksums.files.erase(rename_from);
            }
            else if (new_data_part->checksums.files.count(rename_from))
            {
                new_data_part->checksums.files[rename_to] = new_data_part->checksums.files[rename_from];

                new_data_part->checksums.files.erase(rename_from);
            }
        }

        MergeTreeDataMergerMutator::finalizeMutatedPart(source_part, new_data_part, need_remove_expired_values, compression_codec);
    }

    return new_data_part;
}

void MutateTask::mutateAllPartColumns(
    MergeTreeData::MutableDataPartPtr new_data_part,
    // const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & skip_indices,
    const MergeTreeProjections & projections_to_build,
    BlockInputStreamPtr mutating_stream,
    // time_t time_of_mutation,
    const CompressionCodecPtr & compression_codec,
    MergeListEntry & merge_entry,
    bool need_remove_expired_values,
    bool need_sync
    // ReservationSharedPtr space_reservation,
    // TableLockHolder & holder,
    // ContextPtr context
    )
{
    if (mutating_stream == nullptr)
        throw Exception("Cannot mutate part columns with uninitialized mutations stream. It's a bug", ErrorCodes::LOGICAL_ERROR);

    if (metadata_snapshot->hasPrimaryKey() || metadata_snapshot->hasSecondaryIndices())
        mutating_stream = std::make_shared<MaterializingBlockInputStream>(
            std::make_shared<ExpressionBlockInputStream>(mutating_stream, data.getPrimaryKeyAndSkipIndicesExpression(metadata_snapshot)));

    if (need_remove_expired_values)
        mutating_stream = std::make_shared<TTLBlockInputStream>(mutating_stream, data, metadata_snapshot, new_data_part, time_of_mutation, true);

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();

    MergedBlockOutputStream out{
        new_data_part,
        metadata_snapshot,
        new_data_part->getColumns(),
        skip_indices,
        compression_codec};

    mutating_stream->readPrefix();
    out.writePrefix();



    writeWithProjections(
        data,
        mutator,
        merges_blocker,
        log,
        new_data_part,
        metadata_snapshot,
        projections_to_build,
        mutating_stream,
        out,
        time_of_mutation,
        merge_entry,
        space_reservation,
        holder,
        context,
        minmax_idx.get());

    new_data_part->minmax_idx = std::move(minmax_idx);
    mutating_stream->readSuffix();
    out.writeSuffixAndFinalizePart(new_data_part, need_sync);
}


void MutateTask::mutateSomePartColumns(
    const MergeTreeDataPartPtr & source_part,
    // const StorageMetadataPtr & metadata_snapshot,
    const std::set<MergeTreeIndexPtr> & indices_to_recalc,
    const std::set<MergeTreeProjectionPtr> & projections_to_recalc,
    const Block & mutation_header,
    MergeTreeData::MutableDataPartPtr new_data_part,
    BlockInputStreamPtr mutating_stream,
    // time_t time_of_mutation,
    const CompressionCodecPtr & compression_codec,
    MergeListEntry & merge_entry,
    bool need_remove_expired_values,
    bool need_sync
    // ReservationSharedPtr space_reservation,
    // TableLockHolder & holder,
    // ContextPtr context
    )
{
    if (mutating_stream == nullptr)
        throw Exception("Cannot mutate part columns with uninitialized mutations stream. It's a bug", ErrorCodes::LOGICAL_ERROR);

    if (need_remove_expired_values)
        mutating_stream = std::make_shared<TTLBlockInputStream>(mutating_stream, data, metadata_snapshot, new_data_part, time_of_mutation, true);

    IMergedBlockOutputStream::WrittenOffsetColumns unused_written_offsets;
    MergedColumnOnlyOutputStream out(
        new_data_part,
        metadata_snapshot,
        mutation_header,
        compression_codec,
        std::vector<MergeTreeIndexPtr>(indices_to_recalc.begin(), indices_to_recalc.end()),
        nullptr,
        source_part->index_granularity,
        &source_part->index_granularity_info
    );

    mutating_stream->readPrefix();
    out.writePrefix();

    std::vector<MergeTreeProjectionPtr> projections_to_build(projections_to_recalc.begin(), projections_to_recalc.end());
    writeWithProjections(
        data,
        mutator,
        merges_blocker,
        log,
        new_data_part,
        metadata_snapshot,
        projections_to_build,
        mutating_stream,
        out,
        time_of_mutation,
        merge_entry,
        space_reservation,
        holder,
        context
        );

    mutating_stream->readSuffix();

    auto changed_checksums = out.writeSuffixAndGetChecksums(new_data_part, new_data_part->checksums, need_sync);

    new_data_part->checksums.add(std::move(changed_checksums));
}


class IExecutableTask
{
public:
    virtual bool execute() = 0;
    virtual ~IExecutableTask() = default;
};

using ExecutableTaskUniquePtr = std::unique_ptr<IExecutableTask>;

struct Parameters
{
    MergeTreeData & data;
    MergeTreeDataMergerMutator & mutator;
    ActionBlocker & merges_blocker;
    Poco::Logger * logger;
    MergeTreeData::MutableDataPartPtr new_data_part;
    const StorageMetadataPtr & metadata_snapshot;
    const MergeTreeProjections & projections_to_build;
    BlockInputStreamPtr mutating_stream;
    IMergedBlockOutputStream & out;
    time_t time_of_mutation;
    MergeListEntry & merge_entry;
    ReservationSharedPtr space_reservation;
    TableLockHolder & holder;
    ContextPtr context;
    IMergeTreeDataPart::MinMaxIndex * minmax_idx;
};


class MergeProjectionPartsTask : public IExecutableTask
{
public:
    MergeProjectionPartsTask(
        String name_,
        MergeTreeData::MutableDataPartsVector && parts_,
        const ProjectionDescription & projection_,
        MergeTreeData::MutableDataPartPtr new_data_part_,
        size_t & block_num_,
        Parameters parameters_
        )
        : name(std::move(name_))
        , parts(std::move(parts_))
        , projection(projection_)
        , block_num(block_num_)
        , new_data_part(new_data_part_)
        , parameters(std::move(parameters_))
        , log(&Poco::Logger::get("MergeProjectionPartsTask"))
        {
            LOG_DEBUG(log, "Selected {} projection_parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
            level_parts[current_level] = std::move(parts);
        }

    bool execute() override
    {
        auto & current_level_parts = level_parts[current_level];
        auto & next_level_parts = level_parts[next_level];

        MergeTreeData::MutableDataPartsVector selected_parts;
        while (selected_parts.size() < max_parts_to_merge_in_one_level && !current_level_parts.empty())
        {
            selected_parts.push_back(std::move(current_level_parts.back()));
            current_level_parts.pop_back();
        }

        if (selected_parts.empty())
        {
            if (next_level_parts.empty())
            {
                LOG_WARNING(log, "There is no projection parts merged");

                /// Task is finished
                return false;
            }
            current_level = next_level;
            ++next_level;
        }
        else if (selected_parts.size() == 1)
        {
            if (next_level_parts.empty())
            {
                LOG_DEBUG(log, "Merged a projection part in level {}", current_level);
                selected_parts[0]->renameTo(projection.name + ".proj", true);
                selected_parts[0]->name = projection.name;
                selected_parts[0]->is_temp = false;
                new_data_part->addProjectionPart(name, std::move(selected_parts[0]));

                /// Task is finished
                return false;
            }
            else
            {
                LOG_DEBUG(log, "Forwarded part {} in level {} to next level", selected_parts[0]->name, current_level);
                next_level_parts.push_back(std::move(selected_parts[0]));
            }
        }
        else if (selected_parts.size() > 1)
        {
            // Generate a unique part name
            ++block_num;
            auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
            MergeTreeData::DataPartsVector const_selected_parts(
                std::make_move_iterator(selected_parts.begin()), std::make_move_iterator(selected_parts.end()));
            projection_future_part->assign(std::move(const_selected_parts));
            projection_future_part->name = fmt::format("{}_{}", projection.name, ++block_num);
            projection_future_part->part_info = {"all", 0, 0, 0};

            MergeTreeData::MergingParams projection_merging_params;
            projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
            if (projection.type == ProjectionDescription::Type::Aggregate)
                projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

            LOG_DEBUG(log, "Merged {} parts in level {} to {}", selected_parts.size(), current_level, projection_future_part->name);
            auto tmp_part_merge_task = parameters.mutator.mergePartsToTemporaryPart(
                projection_future_part,
                projection.metadata,
                parameters.merge_entry,
                parameters.holder,
                parameters.time_of_mutation,
                parameters.context,
                parameters.space_reservation,
                false, // TODO Do we need deduplicate for projections
                {},
                projection_merging_params,
                new_data_part.get(),
                "tmp_merge_");

            next_level_parts.push_back(executeHere(tmp_part_merge_task));

            next_level_parts.back()->is_temp = true;
        }

        /// Need execute again
        return true;
    }
private:
    String name;
    MergeTreeData::MutableDataPartsVector parts;
    const ProjectionDescription & projection;
    size_t & block_num;
    MergeTreeData::MutableDataPartPtr new_data_part;
    Parameters parameters;

    Poco::Logger * log;

    std::map<size_t, MergeTreeData::MutableDataPartsVector> level_parts;
    size_t current_level = 0;
    size_t next_level = 1;

    /// TODO(nikitamikhaylov): make this constant a setting
    static constexpr size_t max_parts_to_merge_in_one_level = 10;
};


// This class is responsible for:
// 1. get projection pipeline and a sink to write parts
// 2. build an executor that can write block to the input stream (actually we can write through it to generate as many parts as possible)
// 3. finalize the pipeline so that all parts are merged into one part

// In short it executed a mutation for the part an original part and for its every projection

/**
 *
 * An overview of how the process of mutation works for projections:
 *
 * The mutation for original parts is executed block by block,
 * but additionally we execute a SELECT query for each projection over a current block.
 * And we store results to a map : ProjectionName -> ArrayOfParts.
 *
 * Then, we have to merge all parts for each projection. But we will have constraint:
 * We cannot execute merge on more than 10 parts simulatiously.
 * So we build a tree of merges. At the beginning all the parts have level 0.
 * At each step we take not bigger than 10 parts from the same level
 * and merge it into a bigger part with incremented level.
 */
class PartMergerWriter
{
public:
    PartMergerWriter(Parameters parameters_) : parameters(std::move(parameters_)), projections(parameters.metadata_snapshot->projections)
    {

    }

    bool execute()
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                state = State::NEED_MUTATE_ORIGINAL_PART;
                return true;
            }
            case State::NEED_MUTATE_ORIGINAL_PART:
            {
                if (mutateOriginalPartAndPrepareProjections())
                    return true;

                state = State::NEED_MERGE_PROJECTION_PARTS;
                return true;
            }
            case State::NEED_MERGE_PROJECTION_PARTS:
            {
                if (iterateThroughAllProjections())
                    return true;

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
    }

private:
    void prepare();
    bool mutateOriginalPartAndPrepareProjections();
    bool iterateThroughAllProjections();
    void constructTaskForProjectionPartsMerge();
    void finalize();

    enum class State
    {
        NEED_PREPARE,
        NEED_MUTATE_ORIGINAL_PART,
        NEED_MERGE_PROJECTION_PARTS,

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    Parameters parameters;

    Block block;
    size_t block_num = 0;

    using ProjectionNameToItsBlocks =std::map<String, MergeTreeData::MutableDataPartsVector>;
    ProjectionNameToItsBlocks projection_parts;
    std::move_iterator<ProjectionNameToItsBlocks::iterator> projection_parts_iterator;

    std::vector<SquashingTransform> projection_squashes;
    const ProjectionsDescription & projections;

    ExecutableTaskUniquePtr merge_projection_parts_task_ptr;
};


void PartMergerWriter::prepare()
{
    for (size_t i = 0, size = parameters.projections_to_build.size(); i < size; ++i)
    {
        projection_squashes.emplace_back(65536, 65536 * 256);
    }
}


bool PartMergerWriter::mutateOriginalPartAndPrepareProjections()
{
    if (MutationHelpers::checkOperationIsNotCanceled(parameters.merges_blocker, parameters.merge_entry) && (block = parameters.mutating_stream->read()))
    {
        if (parameters.minmax_idx)
            parameters.minmax_idx->update(block, parameters.data.getMinMaxColumnsNames(parameters.metadata_snapshot->getPartitionKey()));

        parameters.out.write(block);

        for (size_t i = 0, size = parameters.projections_to_build.size(); i < size; ++i)
        {
            const auto & projection = parameters.projections_to_build[i]->projection;
            auto in = InterpreterSelectQuery(
                          projection.query_ast,
                          parameters.context,
                          Pipe(std::make_shared<SourceFromSingleChunk>(block, Chunk(block.getColumns(), block.rows()))),
                          SelectQueryOptions{
                              projection.type == ProjectionDescription::Type::Normal ? QueryProcessingStage::FetchColumns : QueryProcessingStage::WithMergeableState})
                          .execute()
                          .getInputStream();
            in = std::make_shared<SquashingBlockInputStream>(in, block.rows(), std::numeric_limits<UInt64>::max());
            in->readPrefix();
            auto & projection_squash = projection_squashes[i];
            auto projection_block = projection_squash.add(in->read());
            if (in->read())
                throw Exception("Projection cannot increase the number of rows in a block", ErrorCodes::LOGICAL_ERROR);
            in->readSuffix();
            if (projection_block)
            {
                projection_parts[projection.name].emplace_back(
                    MergeTreeDataWriter::writeTempProjectionPart(parameters.data, parameters.logger, projection_block, projection, parameters.new_data_part.get(), ++block_num));
            }
        }

        parameters.merge_entry->rows_written += block.rows();
        parameters.merge_entry->bytes_written_uncompressed += block.bytes();

        /// Need execute again
        return true;
    }

    // Write the last block
    for (size_t i = 0, size = parameters.projections_to_build.size(); i < size; ++i)
    {
        const auto & projection = parameters.projections_to_build[i]->projection;
        auto & projection_squash = projection_squashes[i];
        auto projection_block = projection_squash.add({});
        if (projection_block)
        {
            projection_parts[projection.name].emplace_back(
                MergeTreeDataWriter::writeTempProjectionPart(parameters.data, parameters.logger, projection_block, projection, parameters.new_data_part.get(), ++block_num));
        }
    }

    projection_parts_iterator = std::make_move_iterator(projection_parts.begin());

    /// Maybe there are no projections ?
    if (projection_parts_iterator != std::make_move_iterator(projection_parts.end()))
        constructTaskForProjectionPartsMerge();

    /// Let's move on to the next stage
    return false;
}


void PartMergerWriter::constructTaskForProjectionPartsMerge()
{
    auto && [name, parts] = *projection_parts_iterator;
    const auto & projection = projections.get(name);

    merge_projection_parts_task_ptr = std::make_unique<MergeProjectionPartsTask>
    (
        name,
        std::move(parts),
        projection,
        parameters.new_data_part,
        block_num,
        parameters
    );
}


bool PartMergerWriter::iterateThroughAllProjections()
{
    /// In case if there are no projections we didn't construct a task
    if (!merge_projection_parts_task_ptr)
        return false;

    if (merge_projection_parts_task_ptr->execute())
        return true;

    ++projection_parts_iterator;

    if (projection_parts_iterator == std::make_move_iterator(projection_parts.end()))
        return false;

    constructTaskForProjectionPartsMerge();

    return true;
}


void MutateTask::writeWithProjections(
    MergeTreeData & data_param,
    MergeTreeDataMergerMutator & mutator_param,
    ActionBlocker & merges_blocker_param,
    Poco::Logger * logger,
    MergeTreeData::MutableDataPartPtr new_data_part,
    const StorageMetadataPtr & metadata_snapshot_param,
    const MergeTreeProjections & projections_to_build,
    BlockInputStreamPtr mutating_stream,
    IMergedBlockOutputStream & out,
    time_t time_of_mutation_param,
    MergeListEntry & merge_entry,
    ReservationSharedPtr space_reservation_param,
    TableLockHolder & holder_param,
    ContextPtr context_param,
    IMergeTreeDataPart::MinMaxIndex * minmax_idx
    )
{
    auto task = std::make_unique<PartMergerWriter>
    (Parameters{
        data_param,
        mutator_param,
        merges_blocker_param,
        logger,
        new_data_part,
        metadata_snapshot_param,
        projections_to_build,
        mutating_stream,
        out,
        time_of_mutation_param,
        merge_entry,
        space_reservation_param,
        holder_param,
        context_param,
        minmax_idx
    });

    while (task->execute()) {}
}



}
