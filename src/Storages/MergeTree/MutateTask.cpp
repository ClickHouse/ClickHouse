#include <Storages/MergeTree/MutateTask.h>

#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <Storages/MergeTree/DataPartStorageOnDisk.h>
#include <Columns/ColumnsNumber.h>
#include <Parsers/queryToString.h>
#include <Interpreters/SquashingTransform.h>
#include <Interpreters/MergeTreeTransaction.h>
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
#include <boost/algorithm/string/replace.hpp>


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
            else if (bool has_column = part_columns.has(command.column_name), has_nested_column = part_columns.hasNested(command.column_name); has_column || has_nested_column)
            {
                if (command.type == MutationCommand::Type::DROP_COLUMN || command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    if (has_nested_column)
                    {
                        const auto & nested = part_columns.getNested(command.column_name);
                        assert(!nested.empty());
                        for (const auto & nested_column : nested)
                            mutated_columns.emplace(nested_column.name);
                    }
                    else
                        mutated_columns.emplace(command.column_name);
                }

                if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    for_interpreter.push_back(
                    {
                        .type = MutationCommand::Type::READ_COLUMN,
                        .column_name = command.rename_to,
                    });
                    part_columns.rename(command.column_name, command.rename_to);
                }
            }
        }
        /// If it's compact part, then we don't need to actually remove files
        /// from disk we just don't read dropped columns
        for (const auto & column : part->getColumns())
        {
            if (!mutated_columns.contains(column.name))
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

/// Get the columns list of the resulting part in the same order as storage_columns.
static std::pair<NamesAndTypesList, SerializationInfoByName>
getColumnsForNewDataPart(
    MergeTreeData::DataPartPtr source_part,
    const Block & updated_header,
    NamesAndTypesList storage_columns,
    const SerializationInfoByName & serialization_infos,
    const MutationCommands & commands_for_removes)
{
    NameSet removed_columns;
    NameToNameMap renamed_columns_to_from;
    NameToNameMap renamed_columns_from_to;
    ColumnsDescription part_columns(source_part->getColumns());

    /// All commands are validated in AlterCommand so we don't care about order
    for (const auto & command : commands_for_removes)
    {
        if (command.type == MutationCommand::UPDATE)
        {
            for (const auto & [column_name, _] : command.column_to_update_expression)
            {
                if (column_name == "__row_exists" && !storage_columns.contains(column_name))
                    storage_columns.emplace_back("__row_exists", std::make_shared<DataTypeUInt8>());
            }
        }

        /// If we don't have this column in source part, than we don't need to materialize it
        if (!part_columns.has(command.column_name))
            continue;

        if (command.type == MutationCommand::DROP_COLUMN)
            removed_columns.insert(command.column_name);

        if (command.type == MutationCommand::RENAME_COLUMN)
        {
            renamed_columns_to_from.emplace(command.rename_to, command.column_name);
            renamed_columns_from_to.emplace(command.column_name, command.rename_to);
        }
    }

    SerializationInfoByName new_serialization_infos;
    for (const auto & [name, info] : serialization_infos)
    {
        if (removed_columns.contains(name))
            continue;

        auto it = renamed_columns_from_to.find(name);
        if (it != renamed_columns_from_to.end())
            new_serialization_infos.emplace(it->second, info);
        else
            new_serialization_infos.emplace(name, info);
    }

    /// In compact parts we read all columns, because they all stored in a
    /// single file
    if (!isWidePart(source_part))
        return {updated_header.getNamesAndTypesList(), new_serialization_infos};

    Names source_column_names = source_part->getColumns().getNames();
    NameSet source_columns_name_set(source_column_names.begin(), source_column_names.end());
    for (auto it = storage_columns.begin(); it != storage_columns.end();)
    {
        if (updated_header.has(it->name))
        {
            auto updated_type = updated_header.getByName(it->name).type;
            if (updated_type != it->type)
                it->type = updated_type;
            ++it;
        }
        else
        {
            if (!source_columns_name_set.contains(it->name))
            {
                /// Source part doesn't have column but some other column
                /// was renamed to it's name.
                auto renamed_it = renamed_columns_to_from.find(it->name);
                if (renamed_it != renamed_columns_to_from.end()
                    && source_columns_name_set.contains(renamed_it->second))
                    ++it;
                else
                    it = storage_columns.erase(it);
            }
            else
            {
                /// Check that this column was renamed to some other name
                bool was_renamed = renamed_columns_from_to.contains(it->name);
                bool was_removed = removed_columns.contains(it->name);

                /// If we want to rename this column to some other name, than it
                /// should it's previous version should be dropped or removed
                if (renamed_columns_to_from.contains(it->name) && !was_renamed && !was_removed)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Incorrect mutation commands, trying to rename column {} to {}, but part {} already has column {}", renamed_columns_to_from[it->name], it->name, source_part->name, it->name);

                /// Column was renamed and no other column renamed to it's name
                /// or column is dropped.
                if (!renamed_columns_to_from.contains(it->name) && (was_renamed || was_removed))
                    it = storage_columns.erase(it);
                else
                    ++it;
            }
        }
    }

    return {storage_columns, new_serialization_infos};
}


static ExecuteTTLType shouldExecuteTTL(const StorageMetadataPtr & metadata_snapshot, const ColumnDependencies & dependencies)
{
    if (!metadata_snapshot->hasAnyTTL())
        return ExecuteTTLType::NONE;

    bool has_ttl_expression = false;

    for (const auto & dependency : dependencies)
    {
        if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
            has_ttl_expression = true;

        if (dependency.kind == ColumnDependency::TTL_TARGET)
            return ExecuteTTLType::NORMAL;
    }
    return has_ttl_expression ? ExecuteTTLType::RECALCULATE : ExecuteTTLType::NONE;
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
        if (!removed_indices.contains(index.name))
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
        if (!removed_projections.contains(projection.name))
            new_projections.push_back(&projection);

    return new_projections;
}


/// Return set of indices which should be recalculated during mutation also
/// wraps input stream into additional expression stream
static std::set<MergeTreeIndexPtr> getIndicesToRecalculate(
    QueryPipelineBuilder & builder,
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
        if (!has_index && materialized_indices.contains(index.name))
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
                if (updated_columns.contains(col))
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

    if (!indices_to_recalc.empty() && builder.initialized())
    {
        auto indices_recalc_syntax = TreeRewriter(context).analyze(indices_recalc_expr_list, builder.getHeader().getNamesAndTypesList());
        auto indices_recalc_expr = ExpressionAnalyzer(
                indices_recalc_expr_list,
                indices_recalc_syntax, context).getActions(false);

        /// We can update only one column, but some skip idx expression may depend on several
        /// columns (c1 + c2 * c3). It works because this stream was created with help of
        /// MutationsInterpreter which knows about skip indices and stream 'in' already has
        /// all required columns.
        /// TODO move this logic to single place.
        builder.addTransform(std::make_shared<ExpressionTransform>(builder.getHeader(), indices_recalc_expr));
        builder.addTransform(std::make_shared<MaterializingTransform>(builder.getHeader()));
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
        if (!source_part->checksums.has(projection.name + ".proj") && materialized_projections.contains(projection.name))
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
                if (updated_columns.contains(col))
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

    /// Remove deleted rows mask file name to create hard link for it when mutate some columns.
    if (files_to_skip.contains(IMergeTreeDataPart::DELETED_ROWS_MARK_FILE_NAME))
        files_to_skip.erase(IMergeTreeDataPart::DELETED_ROWS_MARK_FILE_NAME);

    /// Skip updated files
    for (const auto & entry : updated_header)
    {
        ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            String stream_name = ISerialization::getFileNameForStream({entry.name, entry.type}, substream_path);
            files_to_skip.insert(stream_name + ".bin");
            files_to_skip.insert(stream_name + mrk_extension);
        };

        source_part->getSerialization({entry.name, entry.type})->enumerateStreams(callback);
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
        source_part->getSerialization(column)->enumerateStreams(
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
                source_part->getSerialization(*column)->enumerateStreams(callback);
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
                source_part->getSerialization(*column)->enumerateStreams(callback);
        }
    }

    return rename_vector;
}


/// Initialize and write to disk new part fields like checksums, columns, etc.
void finalizeMutatedPart(
    const MergeTreeDataPartPtr & source_part,
    const DataPartStorageBuilderPtr & data_part_storage_builder,
    MergeTreeData::MutableDataPartPtr new_data_part,
    ExecuteTTLType execute_ttl_type,
    const CompressionCodecPtr & codec,
    ContextPtr context)
{
    //auto disk = new_data_part->volume->getDisk();
    //auto part_path = fs::path(new_data_part->getRelativePath());

    if (new_data_part->uuid != UUIDHelpers::Nil)
    {
        auto out = data_part_storage_builder->writeFile(IMergeTreeDataPart::UUID_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_data_part->uuid, out_hashing);
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
    }

    if (execute_ttl_type != ExecuteTTLType::NONE)
    {
        /// Write a file with ttl infos in json format.
        auto out_ttl = data_part_storage_builder->writeFile("ttl.txt", 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out_ttl);
        new_data_part->ttl_infos.write(out_hashing);
        new_data_part->checksums.files["ttl.txt"].file_size = out_hashing.count();
        new_data_part->checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
    }

    if (!new_data_part->getSerializationInfos().empty())
    {
        auto out = data_part_storage_builder->writeFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out);
        new_data_part->getSerializationInfos().writeJSON(out_hashing);
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_hash = out_hashing.getHash();
    }

    {
        /// Write file with checksums.
        auto out_checksums = data_part_storage_builder->writeFile("checksums.txt", 4096, context->getWriteSettings());
        new_data_part->checksums.write(*out_checksums);
    } /// close fd

    {
        auto out = data_part_storage_builder->writeFile(IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096, context->getWriteSettings());
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out);
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = data_part_storage_builder->writeFile("columns.txt", 4096, context->getWriteSettings());
        new_data_part->getColumns().writeText(*out_columns);
    } /// close fd

    new_data_part->rows_count = source_part->rows_count;
    new_data_part->index_granularity = source_part->index_granularity;
    new_data_part->index = source_part->index;
    new_data_part->minmax_idx = source_part->minmax_idx;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadProjections(false, false);
    new_data_part->setBytesOnDisk(new_data_part->data_part_storage->calculateTotalSizeOnDisk());
    new_data_part->default_codec = codec;
    new_data_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();
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

    StoragePtr storage_from_source_part;
    bool is_lightweight_mutation{false};

    StorageMetadataPtr metadata_snapshot;

    MutationCommandsConstPtr commands;
    time_t time_of_mutation;
    ContextPtr context;
    ReservationSharedPtr space_reservation;

    CompressionCodecPtr compression_codec;

    std::unique_ptr<CurrentMetrics::Increment> num_mutations;

    QueryPipelineBuilder mutating_pipeline_builder;
    QueryPipeline mutating_pipeline; // in
    std::unique_ptr<PullingPipelineExecutor> mutating_executor{nullptr};
    ProgressCallback progress_callback;
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

    MergeTreeData::MutableDataPartPtr new_data_part;
    DataPartStorageBuilderPtr data_part_storage_builder;

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

    MergeTreeTransactionPtr txn;

    MergeTreeData::HardlinkedFiles hardlinked_files;
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
        MutationContextPtr ctx_)
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
                auto builder = selected_parts[0]->data_part_storage->getBuilder();
                selected_parts[0]->renameTo(projection.name + ".proj", true, builder);
                selected_parts[0]->name = projection.name;
                selected_parts[0]->is_temp = false;
                builder->commit();
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
                std::make_unique<MergeListElement>((*ctx->mutate_entry)->table_id, projection_future_part, settings),
                *ctx->holder,
                ctx->time_of_mutation,
                ctx->context,
                ctx->space_reservation,
                false, // TODO Do we need deduplicate for projections
                {},
                projection_merging_params,
                NO_TRANSACTION_PTR,
                ctx->new_data_part.get(),
                ctx->data_part_storage_builder.get(),
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
        if (ctx->new_data_part->getType() == MergeTreeDataPartType::InMemory)
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
            {
                auto tmp_part = MergeTreeDataWriter::writeTempProjectionPart(
                    *ctx->data, ctx->log, projection_block, projection, ctx->data_part_storage_builder, ctx->new_data_part.get(), ++block_num);
                tmp_part.builder->commit();
                tmp_part.finalize();
                projection_parts[projection.name].emplace_back(std::move(tmp_part.part));
            }
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
            auto temp_part = MergeTreeDataWriter::writeTempProjectionPart(
                *ctx->data, ctx->log, projection_block, projection, ctx->data_part_storage_builder, ctx->new_data_part.get(), ++block_num);
            temp_part.builder->commit();
            temp_part.finalize();
            projection_parts[projection.name].emplace_back(std::move(temp_part.part));
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
        ctx->data_part_storage_builder->createDirectories();

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
        ctx->compression_codec = ctx->data->getCompressionCodecForPart(ctx->source_part->getBytesOnDisk(), ctx->source_part->ttl_infos, ctx->time_of_mutation);

        auto skip_part_indices = MutationHelpers::getIndicesForNewDataPart(ctx->metadata_snapshot->getSecondaryIndices(), ctx->for_file_renames);
        ctx->projections_to_build = MutationHelpers::getProjectionsForNewDataPart(ctx->metadata_snapshot->getProjections(), ctx->for_file_renames);

        if (!ctx->mutating_pipeline_builder.initialized())
            throw Exception("Cannot mutate part columns with uninitialized mutations stream. It's a bug", ErrorCodes::LOGICAL_ERROR);

        QueryPipelineBuilder builder(std::move(ctx->mutating_pipeline_builder));

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
            ctx->data_part_storage_builder,
            ctx->metadata_snapshot,
            ctx->new_data_part->getColumns(),
            skip_part_indices,
            ctx->compression_codec,
            ctx->txn);

        ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
        /// Is calculated inside MergeProgressCallback.
        ctx->mutating_pipeline.disableProfileEventUpdate();
        ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

        part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
    }


    void finalize()
    {
        ctx->new_data_part->minmax_idx = std::move(ctx->minmax_idx);
        ctx->mutating_executor.reset();
        ctx->mutating_pipeline.reset();

        static_pointer_cast<MergedBlockOutputStream>(ctx->out)->finalizePart(ctx->new_data_part, ctx->need_sync);
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

        ctx->data_part_storage_builder->createDirectories();

        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        TransactionID tid = ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID;
        /// NOTE do not pass context for writing to system.transactions_info_log,
        /// because part may have temporary name (with temporary block numbers). Will write it later.
        ctx->new_data_part->version.setCreationTID(tid, nullptr);
        ctx->new_data_part->storeVersionMetadata();

        NameSet hardlinked_files;
        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->data_part_storage->iterate(); it->isValid(); it->next())
        {
            if (ctx->files_to_skip.contains(it->name()))
                continue;

            String destination;
            String file_name = it->name();

            auto rename_it = std::find_if(ctx->files_to_rename.begin(), ctx->files_to_rename.end(), [&file_name](const auto & rename_pair)
            {
                return rename_pair.first == file_name;
            });

            if (rename_it != ctx->files_to_rename.end())
            {
                if (rename_it->second.empty())
                    continue;
                destination = rename_it->second;
            }
            else
            {
                destination = it->name();
            }

            if (it->isFile())
            {
                ctx->data_part_storage_builder->createHardLinkFrom(
                    *ctx->source_part->data_part_storage, it->name(), destination);
                hardlinked_files.insert(it->name());
            }
            else if (!endsWith(".tmp_proj", it->name())) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                ctx->data_part_storage_builder->createProjection(destination);

                auto projection_data_part_storage = ctx->source_part->data_part_storage->getProjection(destination);
                auto projection_data_part_storage_builder = ctx->data_part_storage_builder->getProjection(destination);

                for (auto p_it = projection_data_part_storage->iterate(); p_it->isValid(); p_it->next())
                {
                    projection_data_part_storage_builder->createHardLinkFrom(
                        *projection_data_part_storage, p_it->name(), p_it->name());
                    hardlinked_files.insert(p_it->name());
                }
            }
        }

        /// Tracking of hardlinked files required for zero-copy replication.
        /// We don't remove them when we delete last copy of source part because
        /// new part can use them.
        ctx->hardlinked_files.source_table_shared_id = ctx->source_part->storage.getTableSharedID();
        ctx->hardlinked_files.source_part_name = ctx->source_part->name;
        ctx->hardlinked_files.hardlinks_from_source_part = hardlinked_files;

        (*ctx->mutate_entry)->columns_written = ctx->storage_columns.size() - ctx->updated_header.columns();

        ctx->new_data_part->checksums = ctx->source_part->checksums;

        ctx->compression_codec = ctx->source_part->default_codec;

        if (ctx->mutating_pipeline_builder.initialized())
        {
            QueryPipelineBuilder builder(std::move(ctx->mutating_pipeline_builder));

            if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
                builder.addTransform(std::make_shared<TTLTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

            if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
                builder.addTransform(std::make_shared<TTLCalcTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

            ctx->out = std::make_shared<MergedColumnOnlyOutputStream>(
                ctx->data_part_storage_builder,
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
            ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
            /// Is calculated inside MergeProgressCallback.
            ctx->mutating_pipeline.disableProfileEventUpdate();
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
                static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out)->fillChecksums(
                    ctx->new_data_part, ctx->new_data_part->checksums);
            ctx->new_data_part->checksums.add(std::move(changed_checksums));

            static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out)->finish(ctx->need_sync);
        }

        for (const auto & [rename_from, rename_to] : ctx->files_to_rename)
        {
            if (rename_to.empty() && ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
            else if (ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files[rename_to] = ctx->new_data_part->checksums.files[rename_from];
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
        }

        MutationHelpers::finalizeMutatedPart(ctx->source_part, ctx->data_part_storage_builder, ctx->new_data_part, ctx->execute_ttl_type, ctx->compression_codec, ctx->context);
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

/// LightweightDeleteTask works for lightweight delete mutate.
/// The MutationsInterpreter returns a simple select like "select _part_offset where predicates".
/// The prepare() and execute() has special logics for LWD mutate.
class LightweightDeleteTask : public IExecutableTask
{
public:

    explicit LightweightDeleteTask(MutationContextPtr ctx_) : ctx(ctx_) {}

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
                execute();

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

        ctx->data_part_storage_builder->createDirectories();

        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        TransactionID tid = ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID;
        /// NOTE do not pass context for writing to system.transactions_info_log,
        /// because part may have temporary name (with temporary block numbers). Will write it later.
        ctx->new_data_part->version.setCreationTID(tid, nullptr);
        ctx->new_data_part->storeVersionMetadata();

        NameSet hardlinked_files;
        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->data_part_storage->iterate(); it->isValid(); it->next())
        {
            if (ctx->files_to_skip.contains(it->name()))
                continue;

            String destination;
            destination = it->name();

            if (it->isFile())
            {
                ctx->data_part_storage_builder->createHardLinkFrom(
                    *ctx->source_part->data_part_storage, it->name(), destination);
                hardlinked_files.insert(it->name());
            }
            else if (!endsWith(".tmp_proj", it->name())) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                ctx->data_part_storage_builder->createProjection(destination);

                auto projection_data_part_storage = ctx->source_part->data_part_storage->getProjection(destination);
                auto projection_data_part_storage_builder = ctx->data_part_storage_builder->getProjection(destination);

                for (auto p_it = projection_data_part_storage->iterate(); p_it->isValid(); p_it->next())
                {
                    projection_data_part_storage_builder->createHardLinkFrom(
                        *projection_data_part_storage, p_it->name(), p_it->name());
                    hardlinked_files.insert(p_it->name());
                }
            }
        }

        /// Tracking of hardlinked files required for zero-copy replication.
        /// We don't remove them when we delete last copy of source part because
        /// new part can use them.
        ctx->hardlinked_files.source_table_shared_id = ctx->source_part->storage.getTableSharedID();
        ctx->hardlinked_files.source_part_name = ctx->source_part->name;
        ctx->hardlinked_files.hardlinks_from_source_part = hardlinked_files;

        /// Only the _delete mask column will be written.
        (*ctx->mutate_entry)->columns_written = 1;

        ctx->new_data_part->checksums = ctx->source_part->checksums;

        ctx->compression_codec = ctx->source_part->default_codec;

        if (ctx->mutating_pipeline_builder.initialized())
        {
            QueryPipelineBuilder builder(std::move(ctx->mutating_pipeline_builder));

            if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
                builder.addTransform(std::make_shared<TTLTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

            if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
                builder.addTransform(std::make_shared<TTLCalcTransform>(builder.getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true));

            ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
            ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
            /// Is calculated inside MergeProgressCallback.
            ctx->mutating_pipeline.disableProfileEventUpdate();
            ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);
        }
    }

    void execute()
    {
        Block block;
        bool has_deleted_rows = false;

        auto new_deleted_rows = ColumnUInt8::create();
        auto & data = new_deleted_rows->getData();

        /// If this part has already applied lightweight mutation, load the past latest bitmap to merge with current bitmap
        if (ctx->source_part->hasLightweightDelete())
        {
            MergeTreeDataPartDeletedMask deleted_mask {};
            if (ctx->source_part->getDeletedMask(deleted_mask))
            {
                const auto & deleted_rows_col = deleted_mask.getDeletedRows();
                const auto & source_data = deleted_rows_col.getData();
                data.insert(source_data.begin(), source_data.begin() + ctx->source_part->rows_count);

                has_deleted_rows = true;
            }
        }

        if (!has_deleted_rows)
            new_deleted_rows->insertManyDefaults(ctx->source_part->rows_count);

        /// Mark the data corresponding to the offset in the as deleted.
        while (MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry) && ctx->mutating_executor && ctx->mutating_executor->pull(block))
        {
            size_t block_rows = block.rows();

            if (block_rows && !has_deleted_rows)
                has_deleted_rows = true;

            const auto & cols = block.getColumns();
            const auto * offset_col = typeid_cast<const ColumnUInt64 *>(cols[0].get());
            const UInt64 * offset = offset_col->getData().data();

            /// Fill 1 for rows in offset
            for (size_t current_row = 0; current_row < block_rows; current_row++)
                data[offset[current_row]] = 1;
        }

        if (has_deleted_rows)
        {
            ctx->new_data_part->writeDeletedMask(ColumnUInt8::Ptr(std::move(new_deleted_rows)));
        }
    }

    void finalize()
    {
        if (ctx->mutating_executor)
        {
            ctx->mutating_executor.reset();
            ctx->mutating_pipeline.reset();
        }

        MutationHelpers::finalizeMutatedPart(ctx->source_part, ctx->data_part_storage_builder, ctx->new_data_part, ctx->execute_ttl_type, ctx->compression_codec, ctx->context);
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
    const MergeTreeTransactionPtr & txn,
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
    ctx->txn = txn;
    ctx->source_part = ctx->future_part->parts[0];
    ctx->storage_from_source_part = std::make_shared<StorageFromMergeTreeDataPart>(ctx->source_part);

    /// part is checked for lightweight delete in selectPartsToMutate().
    ctx->is_lightweight_mutation = ctx->future_part->mutation_type == MutationType::Lightweight;

    /// Empty mutation commands mean that the mutation is killed. Just work as ordinary, clone the part.
    if (ctx->commands->empty())
        ctx->is_lightweight_mutation = false;

    auto storage_snapshot = ctx->storage_from_source_part->getStorageSnapshot(ctx->metadata_snapshot, context_);
    extendObjectColumns(ctx->storage_columns, storage_snapshot->object_columns, /*with_subcolumns=*/ false);
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

    auto context_for_reading = Context::createCopy(ctx->context);
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);
    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    context_for_reading->setSetting("force_index_by_date", false);
    context_for_reading->setSetting("force_primary_key", false);

    for (const auto & command : *ctx->commands)
    {
        if (command.partition == nullptr || ctx->source_part->info.partition_id == ctx->data->getPartitionIDFromQuery(
                command.partition, context_for_reading))
            ctx->commands_for_part.emplace_back(command);
    }

    if (ctx->source_part->isStoredOnDisk() && !ctx->is_lightweight_mutation && !isStorageTouchedByMutations(
        ctx->storage_from_source_part, ctx->metadata_snapshot, ctx->commands_for_part, Context::createCopy(context_for_reading)))
    {
        LOG_TRACE(ctx->log, "Part {} doesn't change up to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
        promise.set_value(ctx->data->cloneAndLoadDataPartOnSameDisk(ctx->source_part, "tmp_clone_", ctx->future_part->part_info, ctx->metadata_snapshot, ctx->txn, &ctx->hardlinked_files, false));
        return false;
    }
    else
    {
        LOG_TRACE(ctx->log, "Mutating part {} to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
    }

    MutationHelpers::splitMutationCommands(ctx->source_part, ctx->commands_for_part, ctx->for_interpreter, ctx->for_file_renames);

    ctx->stage_progress = std::make_unique<MergeStageProgress>(1.0);

    bool need_mutate_all_columns = !isWidePart(ctx->source_part);

    if (!ctx->for_interpreter.empty())
    {
        ctx->interpreter = std::make_unique<MutationsInterpreter>(
            ctx->storage_from_source_part, ctx->metadata_snapshot, ctx->for_interpreter, context_for_reading, true, ctx->is_lightweight_mutation);
        ctx->materialized_indices = ctx->interpreter->grabMaterializedIndices();
        ctx->materialized_projections = ctx->interpreter->grabMaterializedProjections();
        ctx->mutation_kind = ctx->interpreter->getMutationKind();

        /// Skip to apply deleted mask when reading for MutateSomePartColumns.
        need_mutate_all_columns = need_mutate_all_columns || (ctx->mutation_kind == MutationsInterpreter::MutationKind::MUTATE_OTHER && ctx->interpreter->isAffectingAllColumns());
        if (!need_mutate_all_columns && ctx->source_part->hasLightweightDelete() && !ctx->is_lightweight_mutation)
            ctx->interpreter->setSkipDeletedMask(true);

/////
        ctx->interpreter->setSkipDeletedMask(true);
/////

        ctx->mutating_pipeline_builder = ctx->interpreter->execute();
        ctx->updated_header = ctx->interpreter->getUpdatedHeader();
        ctx->progress_callback = MergeProgressCallback((*ctx->mutate_entry)->ptr(), ctx->watch_prev_elapsed, *ctx->stage_progress);
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + ctx->future_part->name, ctx->space_reservation->getDisk(), 0);
    /// FIXME new_data_part is not used in the case when we clone part with cloneAndLoadDataPartOnSameDisk and return false
    /// Is it possible to handle this case earlier?

    auto data_part_storage = std::make_shared<DataPartStorageOnDisk>(
        single_disk_volume,
        ctx->data->getRelativeDataPath(),
        "tmp_mut_" + ctx->future_part->name);

    ctx->data_part_storage_builder = std::make_shared<DataPartStorageBuilderOnDisk>(
        single_disk_volume,
        ctx->data->getRelativeDataPath(),
        "tmp_mut_" + ctx->future_part->name);

    ctx->new_data_part = ctx->data->createPart(
        ctx->future_part->name, ctx->future_part->type, ctx->future_part->part_info, data_part_storage);

    ctx->new_data_part->uuid = ctx->future_part->uuid;
    ctx->new_data_part->is_temp = true;
    ctx->new_data_part->ttl_infos = ctx->source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    ctx->new_data_part->index_granularity_info = ctx->source_part->index_granularity_info;

    if (ctx->is_lightweight_mutation)
    {
        /// The metadata alter will update the metadata snapshot, we should use same as source part.
        ctx->new_data_part->setColumns(ctx->source_part->getColumns());
        ctx->new_data_part->setSerializationInfos(ctx->source_part->getSerializationInfos());
    }
    else
    {
        auto [new_columns, new_infos] = MutationHelpers::getColumnsForNewDataPart(
            ctx->source_part, ctx->updated_header, ctx->storage_columns,
            ctx->source_part->getSerializationInfos(), ctx->commands_for_part);

        ctx->new_data_part->setColumns(new_columns);
        ctx->new_data_part->setSerializationInfos(new_infos);
    }

    ctx->new_data_part->partition.assign(ctx->source_part->partition);

    /// Don't change granularity type while mutating subset of columns
    ctx->mrk_extension = ctx->source_part->index_granularity_info.is_adaptive ? getAdaptiveMrkExtension(ctx->new_data_part->getType())
                                                                         : getNonAdaptiveMrkExtension();

    const auto data_settings = ctx->data->getSettings();
    ctx->need_sync = needSyncPart(ctx->source_part->rows_count, ctx->source_part->getBytesOnDisk(), *data_settings);
    ctx->execute_ttl_type = ExecuteTTLType::NONE;

    if (ctx->mutating_pipeline_builder.initialized())
        ctx->execute_ttl_type = MutationHelpers::shouldExecuteTTL(ctx->metadata_snapshot, ctx->interpreter->getColumnDependencies());

    /// All columns from part are changed and may be some more that were missing before in part
    /// TODO We can materialize compact part without copying data
    if (need_mutate_all_columns)
    {
        task = std::make_unique<MutateAllPartColumnsTask>(ctx);
    }
    else if (ctx->is_lightweight_mutation)
    {
        ctx->files_to_skip = ctx->source_part->getFileNamesWithoutChecksums();

        /// We will modify or create only deleted_row_mask for lightweight delete. Other columns and key values are copied as-is.
        task = std::make_unique<LightweightDeleteTask>(ctx);
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        /// We will modify only some of the columns. Other columns and key values can be copied as-is.
        for (const auto & name_type : ctx->updated_header.getNamesAndTypesList())
            ctx->updated_columns.emplace(name_type.name);

        ctx->indices_to_recalc = MutationHelpers::getIndicesToRecalculate(
            ctx->mutating_pipeline_builder, ctx->updated_columns, ctx->metadata_snapshot, ctx->context, ctx->materialized_indices, ctx->source_part);
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
            promise.set_value(ctx->data->cloneAndLoadDataPartOnSameDisk(ctx->source_part, "tmp_mut_", ctx->future_part->part_info, ctx->metadata_snapshot, ctx->txn, &ctx->hardlinked_files, false));
            return false;
        }

        task = std::make_unique<MutateSomePartColumnsTask>(ctx);
    }

    return true;
}

const MergeTreeData::HardlinkedFiles & MutateTask::getHardlinkedFiles() const
{
    return ctx->hardlinked_files;
}

DataPartStorageBuilderPtr MutateTask::getBuilder() const
{
    return ctx->data_part_storage_builder;
}

}
