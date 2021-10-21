#include <Storages/MergeTree/MutateTask.h>

#include <base/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <Parsers/queryToString.h>
#include <Interpreters/SquashingTransform.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>

namespace CurrentMetrics
{
    extern const Metric PartMutation;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}

namespace MutationHelpers
{

static bool checkOperationIsNotCanceled(ActionBlocker & merges_blocker, MergeListEntry * mutate_entry)
{
    if (merges_blocker.isCancelled() || (*mutate_entry)->is_cancelled)
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
                || command.type == MutationCommand::Type::MATERIALIZE_COLUMN
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
                for (const auto & [column_name, expr] : command.column_to_update_expression)
                    mutated_columns.emplace(column_name);

                if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
                     mutated_columns.emplace(command.column_name);
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
                || command.type == MutationCommand::Type::MATERIALIZE_COLUMN
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


/// Get skip indices, that should exists in the resulting data part.
static MergeTreeIndices getIndicesForNewDataPart(
    const IndicesDescription & all_indices,
    const MutationCommands & commands_for_removes)
{
    NameSet removed_indices;
    for (const auto & command : commands_for_removes)
        if (command.type == MutationCommand::DROP_INDEX)
            removed_indices.insert(command.column_name);

    MergeTreeIndices new_indices;
    for (const auto & index : all_indices)
        if (!removed_indices.count(index.name))
            new_indices.push_back(MergeTreeIndexFactory::instance().get(index));

    return new_indices;
}

static std::vector<ProjectionDescriptionRawPtr> getProjectionsForNewDataPart(
    const ProjectionsDescription & all_projections,
    const MutationCommands & commands_for_removes)
{
    NameSet removed_projections;
    for (const auto & command : commands_for_removes)
        if (command.type == MutationCommand::DROP_PROJECTION)
            removed_projections.insert(command.column_name);

    std::vector<ProjectionDescriptionRawPtr> new_projections;
    for (const auto & projection : all_projections)
        if (!removed_projections.count(projection.name))
            new_projections.push_back(&projection);

    return new_projections;
}


/// Return set of indices which should be recalculated during mutation also
/// wraps input stream into additional expression stream
static std::set<MergeTreeIndexPtr> getIndicesToRecalculate(
    QueryPipeline & pipeline,
    const NameSet & updated_columns,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const NameSet & materialized_indices,
    const MergeTreeData::DataPartPtr & source_part)
{
    /// Checks if columns used in skipping indexes modified.
    const auto & index_factory = MergeTreeIndexFactory::instance();
    std::set<MergeTreeIndexPtr> indices_to_recalc;
    ASTPtr indices_recalc_expr_list = std::make_shared<ASTExpressionList>();
    const auto & indices = metadata_snapshot->getSecondaryIndices();

    for (size_t i = 0; i < indices.size(); ++i)
    {
        const auto & index = indices[i];

        bool has_index =
            source_part->checksums.has(INDEX_FILE_PREFIX + index.name + ".idx") ||
            source_part->checksums.has(INDEX_FILE_PREFIX + index.name + ".idx2");
        // If we ask to materialize and it already exists
        if (!has_index && materialized_indices.count(index.name))
        {
            if (indices_to_recalc.insert(index_factory.get(index)).second)
            {
                ASTPtr expr_list = index.expression_list_ast->clone();
                for (const auto & expr : expr_list->children)
                    indices_recalc_expr_list->children.push_back(expr->clone());
            }
        }
        // If some dependent columns gets mutated
        else
        {
            bool mutate = false;
            const auto & index_cols = index.expression->getRequiredColumns();
            for (const auto & col : index_cols)
            {
                if (updated_columns.count(col))
                {
                    mutate = true;
                    break;
                }
            }
            if (mutate && indices_to_recalc.insert(index_factory.get(index)).second)
            {
                ASTPtr expr_list = index.expression_list_ast->clone();
                for (const auto & expr : expr_list->children)
                    indices_recalc_expr_list->children.push_back(expr->clone());
            }
        }
    }

    if (!indices_to_recalc.empty() && pipeline.initialized())
    {
        auto indices_recalc_syntax = TreeRewriter(context).analyze(indices_recalc_expr_list, pipeline.getHeader().getNamesAndTypesList());
        auto indices_recalc_expr = ExpressionAnalyzer(
                indices_recalc_expr_list,
                indices_recalc_syntax, context).getActions(false);

        /// We can update only one column, but some skip idx expression may depend on several
        /// columns (c1 + c2 * c3). It works because this stream was created with help of
        /// MutationsInterpreter which knows about skip indices and stream 'in' already has
        /// all required columns.
        /// TODO move this logic to single place.
        QueryPipelineBuilder builder;
        builder.init(std::move(pipeline));
        builder.addTransform(std::make_shared<ExpressionTransform>(builder.getHeader(), indices_recalc_expr));
        builder.addTransform(std::make_shared<MaterializingTransform>(builder.getHeader()));
        pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    }
    return indices_to_recalc;
}

std::set<ProjectionDescriptionRawPtr> getProjectionsToRecalculate(
    const NameSet & updated_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & materialized_projections,
    const MergeTreeData::DataPartPtr & source_part)
{
    /// Checks if columns used in projections modified.
    std::set<ProjectionDescriptionRawPtr> projections_to_recalc;
    for (const auto & projection : metadata_snapshot->getProjections())
    {
        // If we ask to materialize and it doesn't exist
        if (!source_part->checksums.has(projection.name + ".proj") && materialized_projections.count(projection.name))
        {
            projections_to_recalc.insert(&projection);
        }
        else
        {
            // If some dependent columns gets mutated
            bool mutate = false;
            const auto & projection_cols = projection.required_columns;
            for (const auto & col : projection_cols)
            {
                if (updated_columns.count(col))
                {
                    mutate = true;
                    break;
                }
            }
            if (mutate)
                projections_to_recalc.insert(&projection);
        }
    }
    return projections_to_recalc;
}


/// Files, that we don't need to remove and don't need to hardlink, for example columns.txt and checksums.txt.
/// Because we will generate new versions of them after we perform mutation.
NameSet collectFilesToSkip(
    const MergeTreeDataPartPtr & source_part,
    const Block & updated_header,
    const std::set<MergeTreeIndexPtr> & indices_to_recalc,
    const String & mrk_extension,
    const std::set<ProjectionDescriptionRawPtr> & projections_to_recalc)
{
    NameSet files_to_skip = source_part->getFileNamesWithoutChecksums();

    /// Skip updated files
    for (const auto & entry : updated_header)
    {
        ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            String stream_name = ISerialization::getFileNameForStream({entry.name, entry.type}, substream_path);
            files_to_skip.insert(stream_name + ".bin");
            files_to_skip.insert(stream_name + mrk_extension);
        };

        auto serialization = source_part->getSerializationForColumn({entry.name, entry.type});
        serialization->enumerateStreams(callback);
    }
    for (const auto & index : indices_to_recalc)
    {
        files_to_skip.insert(index->getFileName() + ".idx");
        files_to_skip.insert(index->getFileName() + mrk_extension);
    }
    for (const auto & projection : projections_to_recalc)
        files_to_skip.insert(projection->getDirectoryName());

    return files_to_skip;
}


/// Apply commands to source_part i.e. remove and rename some columns in
/// source_part and return set of files, that have to be removed or renamed
/// from filesystem and in-memory checksums. Ordered result is important,
/// because we can apply renames that affects each other: x -> z, y -> x.
static NameToNameVector collectFilesForRenames(
    MergeTreeData::DataPartPtr source_part, const MutationCommands & commands_for_removes, const String & mrk_extension)
{
    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    std::map<String, size_t> stream_counts;
    for (const auto & column : source_part->getColumns())
    {
        auto serialization = source_part->getSerializationForColumn(column);
        serialization->enumerateStreams(
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                ++stream_counts[ISerialization::getFileNameForStream(column, substream_path)];
            });
    }

    NameToNameVector rename_vector;
    /// Remove old data
    for (const auto & command : commands_for_removes)
    {
        if (command.type == MutationCommand::Type::DROP_INDEX)
        {
            if (source_part->checksums.has(INDEX_FILE_PREFIX + command.column_name + ".idx2"))
            {
                rename_vector.emplace_back(INDEX_FILE_PREFIX + command.column_name + ".idx2", "");
                rename_vector.emplace_back(INDEX_FILE_PREFIX + command.column_name + mrk_extension, "");
            }
            else if (source_part->checksums.has(INDEX_FILE_PREFIX + command.column_name + ".idx"))
            {
                rename_vector.emplace_back(INDEX_FILE_PREFIX + command.column_name + ".idx", "");
                rename_vector.emplace_back(INDEX_FILE_PREFIX + command.column_name + mrk_extension, "");
            }
        }
        else if (command.type == MutationCommand::Type::DROP_PROJECTION)
        {
            if (source_part->checksums.has(command.column_name + ".proj"))
                rename_vector.emplace_back(command.column_name + ".proj", "");
        }
        else if (command.type == MutationCommand::Type::DROP_COLUMN)
        {
            ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                String stream_name = ISerialization::getFileNameForStream({command.column_name, command.data_type}, substream_path);
                /// Delete files if they are no longer shared with another column.
                if (--stream_counts[stream_name] == 0)
                {
                    rename_vector.emplace_back(stream_name + ".bin", "");
                    rename_vector.emplace_back(stream_name + mrk_extension, "");
                }
            };

            auto column = source_part->getColumns().tryGetByName(command.column_name);
            if (column)
            {
                auto serialization = source_part->getSerializationForColumn(*column);
                serialization->enumerateStreams(callback);
            }
        }
        else if (command.type == MutationCommand::Type::RENAME_COLUMN)
        {
            String escaped_name_from = escapeForFileName(command.column_name);
            String escaped_name_to = escapeForFileName(command.rename_to);

            ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                String stream_from = ISerialization::getFileNameForStream({command.column_name, command.data_type}, substream_path);

                String stream_to = boost::replace_first_copy(stream_from, escaped_name_from, escaped_name_to);

                if (stream_from != stream_to)
                {
                    rename_vector.emplace_back(stream_from + ".bin", stream_to + ".bin");
                    rename_vector.emplace_back(stream_from + mrk_extension, stream_to + mrk_extension);
                }
            };

            auto column = source_part->getColumns().tryGetByName(command.column_name);
            if (column)
            {
                auto serialization = source_part->getSerializationForColumn(*column);
                serialization->enumerateStreams(callback);
            }
        }
    }

    return rename_vector;
}


/// Initialize and write to disk new part fields like checksums, columns, etc.
void finalizeMutatedPart(
    const MergeTreeDataPartPtr & source_part,
    MergeTreeData::MutableDataPartPtr new_data_part,
    ExecuteTTLType execute_ttl_type,
    const CompressionCodecPtr & codec)
{
    auto disk = new_data_part->volume->getDisk();

    if (new_data_part->uuid != UUIDHelpers::Nil)
    {
        auto out = disk->writeFile(new_data_part->getFullRelativePath() + IMergeTreeDataPart::UUID_FILE_NAME, 4096);
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_data_part->uuid, out_hashing);
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
    }

    if (execute_ttl_type != ExecuteTTLType::NONE)
    {
        /// Write a file with ttl infos in json format.
        auto out_ttl = disk->writeFile(fs::path(new_data_part->getFullRelativePath()) / "ttl.txt", 4096);
        HashingWriteBuffer out_hashing(*out_ttl);
        new_data_part->ttl_infos.write(out_hashing);
        new_data_part->checksums.files["ttl.txt"].file_size = out_hashing.count();
        new_data_part->checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
    }

    {
        /// Write file with checksums.
        auto out_checksums = disk->writeFile(fs::path(new_data_part->getFullRelativePath()) / "checksums.txt", 4096);
        new_data_part->checksums.write(*out_checksums);
    } /// close fd

    {
        auto out = disk->writeFile(new_data_part->getFullRelativePath() + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096);
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out);
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = disk->writeFile(fs::path(new_data_part->getFullRelativePath()) / "columns.txt", 4096);
        new_data_part->getColumns().writeText(*out_columns);
    } /// close fd

    new_data_part->rows_count = source_part->rows_count;
    new_data_part->index_granularity = source_part->index_granularity;
    new_data_part->index = source_part->index;
    new_data_part->minmax_idx = source_part->minmax_idx;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadProjections(false, false);
    new_data_part->setBytesOnDisk(
        MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_data_part->volume->getDisk(), new_data_part->getFullRelativePath()));
    new_data_part->default_codec = codec;
    new_data_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();
    new_data_part->storage.lockSharedData(*new_data_part);
}

}

struct MutationContext
{
    MergeTreeData * data;
    MergeTreeDataMergerMutator * mutator;
    ActionBlocker * merges_blocker;
    TableLockHolder * holder;
    MergeListEntry * mutate_entry;

    Poco::Logger * log{&Poco::Logger::get("MutateTask")};

    FutureMergedMutatedPartPtr future_part;
    MergeTreeData::DataPartPtr source_part;

    StorageMetadataPtr metadata_snapshot;
    MutationCommandsConstPtr commands;
    time_t time_of_mutation;
    ContextPtr context;
    ReservationSharedPtr space_reservation;

    CompressionCodecPtr compression_codec;

    std::unique_ptr<CurrentMetrics::Increment> num_mutations;

    QueryPipeline mutating_pipeline; // in
    std::unique_ptr<PullingPipelineExecutor> mutating_executor;
    Block updated_header;

    std::unique_ptr<MutationsInterpreter> interpreter;
    UInt64 watch_prev_elapsed{0};
    std::unique_ptr<MergeStageProgress> stage_progress{nullptr};

    MutationCommands commands_for_part;
    MutationCommands for_interpreter;
    MutationCommands for_file_renames;

    NamesAndTypesList storage_columns;
    NameSet materialized_indices;
    NameSet materialized_projections;
    MutationsInterpreter::MutationKind::MutationKindEnum mutation_kind
        = MutationsInterpreter::MutationKind::MutationKindEnum::MUTATE_UNKNOWN;

    VolumePtr single_disk_volume;
    MergeTreeData::MutableDataPartPtr new_data_part;
    DiskPtr disk;
    String new_part_tmp_path;

    SyncGuardPtr sync_guard;
    IMergedBlockOutputStreamPtr out{nullptr};

    String mrk_extension;

    std::vector<ProjectionDescriptionRawPtr> projections_to_build;
    IMergeTreeDataPart::MinMaxIndexPtr minmax_idx{nullptr};

    NameSet updated_columns;
    std::set<MergeTreeIndexPtr> indices_to_recalc;
    std::set<ProjectionDescriptionRawPtr> projections_to_recalc;
    NameSet files_to_skip;
    NameToNameVector files_to_rename;

    bool need_sync;
    ExecuteTTLType execute_ttl_type{ExecuteTTLType::NONE};
};

using MutationContextPtr = std::shared_ptr<MutationContext>;


class MergeProjectionPartsTask : public IExecutableTask
{
public:

    MergeProjectionPartsTask(
        String name_,
        MergeTreeData::MutableDataPartsVector && parts_,
        const ProjectionDescription & projection_,
        size_t & block_num_,
        MutationContextPtr ctx_
        )
        : name(std::move(name_))
        , parts(std::move(parts_))
        , projection(projection_)
        , block_num(block_num_)
        , ctx(ctx_)
        , log(&Poco::Logger::get("MergeProjectionPartsTask"))
        {
            LOG_DEBUG(log, "Selected {} projection_parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
            level_parts[current_level] = std::move(parts);
        }

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    UInt64 getPriority() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
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
                ctx->new_data_part->addProjectionPart(name, std::move(selected_parts[0]));

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

            const Settings & settings = ctx->context->getSettingsRef();

            LOG_DEBUG(log, "Merged {} parts in level {} to {}", selected_parts.size(), current_level, projection_future_part->name);
            auto tmp_part_merge_task = ctx->mutator->mergePartsToTemporaryPart(
                projection_future_part,
                projection.metadata,
                ctx->mutate_entry,
                std::make_unique<MergeListElement>(
                    (*ctx->mutate_entry)->table_id,
                    projection_future_part,
                    settings.memory_profiler_step,
                    settings.memory_profiler_sample_probability,
                    settings.max_untracked_memory),
                *ctx->holder,
                ctx->time_of_mutation,
                ctx->context,
                ctx->space_reservation,
                false, // TODO Do we need deduplicate for projections
                {},
                projection_merging_params,
                ctx->new_data_part.get(),
                ".tmp_proj");

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
    MutationContextPtr ctx;

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

    explicit PartMergerWriter(MutationContextPtr ctx_)
        : ctx(ctx_), projections(ctx->metadata_snapshot->projections)
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
        return false;
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
    MutationContextPtr ctx;

    size_t block_num = 0;

    using ProjectionNameToItsBlocks = std::map<String, MergeTreeData::MutableDataPartsVector>;
    ProjectionNameToItsBlocks projection_parts;
    std::move_iterator<ProjectionNameToItsBlocks::iterator> projection_parts_iterator;

    std::vector<SquashingTransform> projection_squashes;
    const ProjectionsDescription & projections;

    ExecutableTaskPtr merge_projection_parts_task_ptr;
};


void PartMergerWriter::prepare()
{
    const auto & settings = ctx->context->getSettingsRef();

    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        // If the parent part is an in-memory part, squash projection output into one block and
        // build in-memory projection because we don't support merging into a new in-memory part.
        // Otherwise we split the materialization into multiple stages similar to the process of
        // INSERT SELECT query.
        if (ctx->new_data_part->getType() == MergeTreeDataPartType::IN_MEMORY)
            projection_squashes.emplace_back(0, 0);
        else
            projection_squashes.emplace_back(settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);
    }
}


bool PartMergerWriter::mutateOriginalPartAndPrepareProjections()
{
    Block cur_block;
    if (MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry) && ctx->mutating_executor->pull(cur_block))
    {
        if (ctx->minmax_idx)
            ctx->minmax_idx->update(cur_block, ctx->data->getMinMaxColumnsNames(ctx->metadata_snapshot->getPartitionKey()));

        ctx->out->write(cur_block);

        for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
        {
            const auto & projection = *ctx->projections_to_build[i];
            auto projection_block = projection_squashes[i].add(projection.calculate(cur_block, ctx->context));
            if (projection_block)
                projection_parts[projection.name].emplace_back(MergeTreeDataWriter::writeTempProjectionPart(
                    *ctx->data, ctx->log, projection_block, projection, ctx->new_data_part.get(), ++block_num));
        }

        (*ctx->mutate_entry)->rows_written += cur_block.rows();
        (*ctx->mutate_entry)->bytes_written_uncompressed += cur_block.bytes();

        /// Need execute again
        return true;
    }

    // Write the last block
    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        const auto & projection = *ctx->projections_to_build[i];
        auto & projection_squash = projection_squashes[i];
        auto projection_block = projection_squash.add({});
        if (projection_block)
        {
            projection_parts[projection.name].emplace_back(MergeTreeDataWriter::writeTempProjectionPart(
                *ctx->data, ctx->log, projection_block, projection, ctx->new_data_part.get(), ++block_num));
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
        block_num,
        ctx
    );
}


bool PartMergerWriter::iterateThroughAllProjections()
{
    /// In case if there are no projections we didn't construct a task
    if (!merge_projection_parts_task_ptr)
        return false;

    if (merge_projection_parts_task_ptr->executeStep())
        return true;

    ++projection_parts_iterator;

    if (projection_parts_iterator == std::make_move_iterator(projection_parts.end()))
        return false;

    constructTaskForProjectionPartsMerge();

    return true;
}

class MutateAllPartColumnsTask : public IExecutableTask
{
public:

    explicit MutateAllPartColumnsTask(MutationContextPtr ctx_) : ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    UInt64 getPriority() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                state = State::NEED_EXECUTE;
                return true;
            }
            case State::NEED_EXECUTE:
            {
                if (part_merger_writer_task->execute())
                    return true;

                state = State::NEED_FINALIZE;
                return true;
            }
            case State::NEED_FINALIZE:
            {
                finalize();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

private:

    void prepare()
    {
        ctx->disk->createDirectories(ctx->new_part_tmp_path);

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
        ctx->compression_codec = ctx->data->getCompressionCodecForPart(ctx->source_part->getBytesOnDisk(), ctx->source_part->ttl_infos, ctx->time_of_mutation);

        auto skip_part_indices = MutationHelpers::getIndicesForNewDataPart(ctx->metadata_snapshot->getSecondaryIndices(), ctx->for_file_renames);
        ctx->projections_to_build = MutationHelpers::getProjectionsForNewDataPart(ctx->metadata_snapshot->getProjections(), ctx->for_file_renames);

        if (!ctx->mutating_pipeline.initialized())
            throw Exception("Cannot mutate part columns with uninitialized mutations stream. It's a bug", ErrorCodes::LOGICAL_ERROR);

        QueryPipelineBuilder builder;
        builder.init(std::move(ctx->mutating_pipeline));

        if (ctx->metadata_snapshot->hasPrimaryKey() || ctx->metadata_snapshot->hasSecondaryIndices())
        {
            builder.addTransform(
                std::make_shared<ExpressionTransform>(builder.getHeader(), ctx->data->getPrimaryKeyAndSkipIndicesExpression(ctx->metadata_snapshot)));

            builder.addTransform(std::make_shared<MaterializingTransform>(builder.getHeader()));
        }

        if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
            builder.addTransform(std::make_shared<TTLTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

        if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
            builder.addTransform(std::make_shared<TTLCalcTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

        ctx->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();

        ctx->out = std::make_shared<MergedBlockOutputStream>(
            ctx->new_data_part,
            ctx->metadata_snapshot,
            ctx->new_data_part->getColumns(),
            skip_part_indices,
            ctx->compression_codec);

        ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

        part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
    }


    void finalize()
    {
        ctx->new_data_part->minmax_idx = std::move(ctx->minmax_idx);
        ctx->mutating_executor.reset();
        ctx->mutating_pipeline.reset();

        static_pointer_cast<MergedBlockOutputStream>(ctx->out)->writeSuffixAndFinalizePart(ctx->new_data_part, ctx->need_sync);
        ctx->out.reset();
    }

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};

    MutationContextPtr ctx;

    std::unique_ptr<PartMergerWriter> part_merger_writer_task;
};

class MutateSomePartColumnsTask : public IExecutableTask
{
public:
    explicit MutateSomePartColumnsTask(MutationContextPtr ctx_) : ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    UInt64 getPriority() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                state = State::NEED_EXECUTE;
                return true;
            }
            case State::NEED_EXECUTE:
            {
                if (part_merger_writer_task && part_merger_writer_task->execute())
                    return true;

                state = State::NEED_FINALIZE;
                return true;
            }
            case State::NEED_FINALIZE:
            {
                finalize();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

private:

    void prepare()
    {
        if (ctx->execute_ttl_type != ExecuteTTLType::NONE)
            ctx->files_to_skip.insert("ttl.txt");

        ctx->disk->createDirectories(ctx->new_part_tmp_path);

        /// Create hardlinks for unchanged files
        for (auto it = ctx->disk->iterateDirectory(ctx->source_part->getFullRelativePath()); it->isValid(); it->next())
        {
            if (ctx->files_to_skip.count(it->name()))
                continue;

            String destination = ctx->new_part_tmp_path;
            String file_name = it->name();

            auto rename_it = std::find_if(ctx->files_to_rename.begin(), ctx->files_to_rename.end(), [&file_name](const auto & rename_pair)
            {
                return rename_pair.first == file_name;
            });
            if (rename_it != ctx->files_to_rename.end())
            {
                if (rename_it->second.empty())
                    continue;
                destination += rename_it->second;
            }
            else
            {
                destination += it->name();
            }

            if (!ctx->disk->isDirectory(it->path()))
                ctx->disk->createHardLink(it->path(), destination);
            else if (!endsWith(".tmp_proj", it->name())) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                ctx->disk->createDirectories(destination);
                for (auto p_it = ctx->disk->iterateDirectory(it->path()); p_it->isValid(); p_it->next())
                {
                    String p_destination = destination + "/";
                    String p_file_name = p_it->name();
                    p_destination += p_it->name();
                    ctx->disk->createHardLink(p_it->path(), p_destination);
                }
            }
        }

        (*ctx->mutate_entry)->columns_written = ctx->storage_columns.size() - ctx->updated_header.columns();

        ctx->new_data_part->checksums = ctx->source_part->checksums;

        ctx->compression_codec = ctx->source_part->default_codec;

        if (ctx->mutating_pipeline.initialized())
        {
            QueryPipelineBuilder builder;
            builder.init(std::move(ctx->mutating_pipeline));

            if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
                builder.addTransform(std::make_shared<TTLTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

            if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
                builder.addTransform(std::make_shared<TTLCalcTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

            ctx->out = std::make_shared<MergedColumnOnlyOutputStream>(
                ctx->new_data_part,
                ctx->metadata_snapshot,
                ctx->updated_header,
                ctx->compression_codec,
                std::vector<MergeTreeIndexPtr>(ctx->indices_to_recalc.begin(), ctx->indices_to_recalc.end()),
                nullptr,
                ctx->source_part->index_granularity,
                &ctx->source_part->index_granularity_info
            );

            ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
            ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

            ctx->projections_to_build = std::vector<ProjectionDescriptionRawPtr>{ctx->projections_to_recalc.begin(), ctx->projections_to_recalc.end()};

            part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
        }
    }


    void finalize()
    {
        if (ctx->mutating_executor)
        {
            ctx->mutating_executor.reset();
            ctx->mutating_pipeline.reset();

            auto changed_checksums =
                static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out)->writeSuffixAndGetChecksums(
                    ctx->new_data_part, ctx->new_data_part->checksums, ctx->need_sync);
            ctx->new_data_part->checksums.add(std::move(changed_checksums));
        }

        for (const auto & [rename_from, rename_to] : ctx->files_to_rename)
        {
            if (rename_to.empty() && ctx->new_data_part->checksums.files.count(rename_from))
            {
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
            else if (ctx->new_data_part->checksums.files.count(rename_from))
            {
                ctx->new_data_part->checksums.files[rename_to] = ctx->new_data_part->checksums.files[rename_from];
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
        }

        MutationHelpers::finalizeMutatedPart(ctx->source_part, ctx->new_data_part, ctx->execute_ttl_type, ctx->compression_codec);
    }


    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    MutationContextPtr ctx;
    MergedColumnOnlyOutputStreamPtr out;

    std::unique_ptr<PartMergerWriter> part_merger_writer_task{nullptr};
};


MutateTask::MutateTask(
    FutureMergedMutatedPartPtr future_part_,
    StorageMetadataPtr metadata_snapshot_,
    MutationCommandsConstPtr commands_,
    MergeListEntry * mutate_entry_,
    time_t time_of_mutation_,
    ContextPtr context_,
    ReservationSharedPtr space_reservation_,
    TableLockHolder & table_lock_holder_,
    MergeTreeData & data_,
    MergeTreeDataMergerMutator & mutator_,
    ActionBlocker & merges_blocker_)
    : ctx(std::make_shared<MutationContext>())
{
    ctx->data = &data_;
    ctx->mutator = &mutator_;
    ctx->merges_blocker = &merges_blocker_;
    ctx->holder = &table_lock_holder_;
    ctx->mutate_entry = mutate_entry_;
    ctx->commands = commands_;
    ctx->context = context_;
    ctx->time_of_mutation = time_of_mutation_;
    ctx->future_part = future_part_;
    ctx->metadata_snapshot = metadata_snapshot_;
    ctx->space_reservation = space_reservation_;
    ctx->storage_columns = metadata_snapshot_->getColumns().getAllPhysical();
}


bool MutateTask::execute()
{

    switch (state)
    {
        case State::NEED_PREPARE:
        {
            if (!prepare())
                return false;

            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE:
        {
            if (task->executeStep())
                return true;

            promise.set_value(ctx->new_data_part);
            return false;
        }
    }
    return false;
}


bool MutateTask::prepare()
{
    MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry);

    if (ctx->future_part->parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to mutate {} parts, not one. "
            "This is a bug.", toString(ctx->future_part->parts.size()));

    ctx->num_mutations = std::make_unique<CurrentMetrics::Increment>(CurrentMetrics::PartMutation);
    ctx->source_part = ctx->future_part->parts[0];
    auto storage_from_source_part = StorageFromMergeTreeDataPart::create(ctx->source_part);

    auto context_for_reading = Context::createCopy(ctx->context);
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);
    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    context_for_reading->setSetting("force_index_by_date", false);
    context_for_reading->setSetting("force_primary_key", false);

    for (const auto & command : *ctx->commands)
    {
        if (command.partition == nullptr || ctx->future_part->parts[0]->info.partition_id == ctx->data->getPartitionIDFromQuery(
                command.partition, context_for_reading))
            ctx->commands_for_part.emplace_back(command);
    }

    if (ctx->source_part->isStoredOnDisk() && !isStorageTouchedByMutations(
        storage_from_source_part, ctx->metadata_snapshot, ctx->commands_for_part, Context::createCopy(context_for_reading)))
    {
        LOG_TRACE(ctx->log, "Part {} doesn't change up to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
        promise.set_value(ctx->data->cloneAndLoadDataPartOnSameDisk(ctx->source_part, "tmp_clone_", ctx->future_part->part_info, ctx->metadata_snapshot));
        return false;
    }
    else
    {
        LOG_TRACE(ctx->log, "Mutating part {} to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
    }

    MutationHelpers::splitMutationCommands(ctx->source_part, ctx->commands_for_part, ctx->for_interpreter, ctx->for_file_renames);

    ctx->stage_progress = std::make_unique<MergeStageProgress>(1.0);

    if (!ctx->for_interpreter.empty())
    {
        ctx->interpreter = std::make_unique<MutationsInterpreter>(
            storage_from_source_part, ctx->metadata_snapshot, ctx->for_interpreter, context_for_reading, true);
        ctx->materialized_indices = ctx->interpreter->grabMaterializedIndices();
        ctx->materialized_projections = ctx->interpreter->grabMaterializedProjections();
        ctx->mutation_kind = ctx->interpreter->getMutationKind();
        ctx->mutating_pipeline = ctx->interpreter->execute();
        ctx->updated_header = ctx->interpreter->getUpdatedHeader();
        ctx->mutating_pipeline.setProgressCallback(MergeProgressCallback((*ctx->mutate_entry)->ptr(), ctx->watch_prev_elapsed, *ctx->stage_progress));
    }

    ctx->single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + ctx->future_part->name, ctx->space_reservation->getDisk(), 0);
    ctx->new_data_part = ctx->data->createPart(
        ctx->future_part->name, ctx->future_part->type, ctx->future_part->part_info, ctx->single_disk_volume, "tmp_mut_" + ctx->future_part->name);

    ctx->new_data_part->uuid = ctx->future_part->uuid;
    ctx->new_data_part->is_temp = true;
    ctx->new_data_part->ttl_infos = ctx->source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    ctx->new_data_part->index_granularity_info = ctx->source_part->index_granularity_info;
    ctx->new_data_part->setColumns(MergeTreeDataMergerMutator::getColumnsForNewDataPart(ctx->source_part, ctx->updated_header, ctx->storage_columns, ctx->for_file_renames));
    ctx->new_data_part->partition.assign(ctx->source_part->partition);

    ctx->disk = ctx->new_data_part->volume->getDisk();
    ctx->new_part_tmp_path = ctx->new_data_part->getFullRelativePath();

    if (ctx->data->getSettings()->fsync_part_directory)
        ctx->sync_guard = ctx->disk->getDirectorySyncGuard(ctx->new_part_tmp_path);

    /// Don't change granularity type while mutating subset of columns
    ctx->mrk_extension = ctx->source_part->index_granularity_info.is_adaptive ? getAdaptiveMrkExtension(ctx->new_data_part->getType())
                                                                         : getNonAdaptiveMrkExtension();

    const auto data_settings = ctx->data->  getSettings();
    ctx->need_sync = needSyncPart(ctx->source_part->rows_count, ctx->source_part->getBytesOnDisk(), *data_settings);
    ctx->execute_ttl_type = ExecuteTTLType::NONE;

    if (ctx->mutating_pipeline.initialized())
        ctx->execute_ttl_type = MergeTreeDataMergerMutator::shouldExecuteTTL(ctx->metadata_snapshot, ctx->interpreter->getColumnDependencies());


    /// All columns from part are changed and may be some more that were missing before in part
    /// TODO We can materialize compact part without copying data
    if (!isWidePart(ctx->source_part)
        || (ctx->mutation_kind == MutationsInterpreter::MutationKind::MUTATE_OTHER && ctx->interpreter && ctx->interpreter->isAffectingAllColumns()))
    {
        task = std::make_unique<MutateAllPartColumnsTask>(ctx);
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {

        /// We will modify only some of the columns. Other columns and key values can be copied as-is.
        for (const auto & name_type : ctx->updated_header.getNamesAndTypesList())
            ctx->updated_columns.emplace(name_type.name);

        ctx->indices_to_recalc = MutationHelpers::getIndicesToRecalculate(
            ctx->mutating_pipeline, ctx->updated_columns, ctx->metadata_snapshot, ctx->context, ctx->materialized_indices, ctx->source_part);
        ctx->projections_to_recalc = MutationHelpers::getProjectionsToRecalculate(
            ctx->updated_columns, ctx->metadata_snapshot, ctx->materialized_projections, ctx->source_part);

        ctx->files_to_skip = MutationHelpers::collectFilesToSkip(
            ctx->source_part,
            ctx->updated_header,
            ctx->indices_to_recalc,
            ctx->mrk_extension,
            ctx->projections_to_recalc);
        ctx->files_to_rename = MutationHelpers::collectFilesForRenames(ctx->source_part, ctx->for_file_renames, ctx->mrk_extension);

        if (ctx->indices_to_recalc.empty() &&
            ctx->projections_to_recalc.empty() &&
            ctx->mutation_kind != MutationsInterpreter::MutationKind::MUTATE_OTHER
            && ctx->files_to_rename.empty())
        {
            LOG_TRACE(ctx->log, "Part {} doesn't change up to mutation version {} (optimized)", ctx->source_part->name, ctx->future_part->part_info.mutation);
            promise.set_value(ctx->data->cloneAndLoadDataPartOnSameDisk(ctx->source_part, "tmp_clone_", ctx->future_part->part_info, ctx->metadata_snapshot));
            return false;
        }

        task = std::make_unique<MutateSomePartColumnsTask>(ctx);
    }

    return true;
}


}
