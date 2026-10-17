#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedColumnDependencies.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <queue>
#include <ranges>

namespace ProfileEvents
{
    extern const Event ReadTasksWithAppliedPatches;
    extern const Event PatchesAppliedInAllReadTasks;
    extern const Event PatchesMergeAppliedInAllReadTasks;
    extern const Event PatchesJoinAppliedInAllReadTasks;
    extern const Event PatchesMergeOnKeyAppliedInAllReadTasks;
    extern const Event ReadTasksWithAppliedMutationsOnFly;
    extern const Event MutationsAppliedOnFlyInAllReadTasks;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested;
    extern const SettingsBool analyzer_compatibility_prefer_alias_over_subcolumn;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

/// Recreates ALTER UPDATE command but with assignment
/// list that contains only columns from @available_columns.
/// Example:
/// If we have mutation "UPDATE c1 = 'x', c2 = 'y' WHERE <cond>"
/// and we read only column "c1" in query then we need
/// to reacreate mutation command as "UPDATE c1 = 'x' WHERE <cond>"
static MutationCommand createCommandWithUpdatedColumns(
    const MutationCommand & command,
    const std::unordered_map<String, ASTPtr> & available_columns)
{
    chassert(command.type == MutationCommand::Type::UPDATE);
    chassert(!command.ast_text.empty());

    MutationCommand res;
    res.type = command.type;
    res.mutation_version = command.mutation_version;
    res.max_parser_depth = command.max_parser_depth;
    res.max_parser_backtracks = command.max_parser_backtracks;
    res.ast_text = command.ast_text;

    auto handle = res.mutateAst();
    auto new_assignments = make_intrusive<ASTExpressionList>();

    for (const auto & child : handle->update_assignments->children)
    {
        const auto & assignment = assert_cast<const ASTAssignment &>(*child);
        if (available_columns.contains(assignment.column_name))
            new_assignments->children.push_back(child->clone());
    }

    handle->update_assignments = handle->children.emplace_back(std::move(new_assignments)).get();
    handle.commit();
    return res;
}

static bool isLightweightDeleteCommand(const String & column_name, const ASTPtr & ast)
{
    if (column_name != RowExistsColumn::name)
        return false;

    const auto * literal = ast->as<ASTLiteral>();
    if (!literal)
        return false;

    if (literal->value.getType() != Field::Types::UInt64)
        return false;

    return literal->value.safeGet<UInt64>() == 0;
}

static MutationCommand createLightweightDeleteCommand(const MutationCommand & command)
{
    chassert(command.type == MutationCommand::Type::UPDATE);
    auto src_alter = command.ast();
    chassert(src_alter && src_alter->predicate != nullptr);

    auto alter_command = make_intrusive<ASTAlterCommand>();
    alter_command->type = ASTAlterCommand::DELETE;

    if (src_alter->partition)
        alter_command->partition = alter_command->children.emplace_back(src_alter->partition->clone()).get();

    if (src_alter->partitions)
        alter_command->partitions = alter_command->children.emplace_back(src_alter->partitions->clone()).get();

    alter_command->predicate = alter_command->children.emplace_back(src_alter->predicate->clone()).get();
    auto mutation_command = MutationCommand::parse(
        *alter_command,
        /* parse_alter_commands = */ false,
        /* with_pure_metadata_commands = */ false,
        command.max_parser_depth,
        command.max_parser_backtracks);

    if (!mutation_command)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse command {}", alter_command->formatForErrorMessage());

    /// The rewritten command stands for the same mutation, so it carries its version - as
    /// `createCommandWithUpdatedColumns` does for its own rewrite. Without it the on-fly stage built
    /// from this command has no version bound, `getMaxPatchVersionForStep` returns none, and the
    /// patch-visibility window of the stage is unbounded above: a patch part created *after* this
    /// `DELETE` would be applied before its predicate is evaluated.
    mutation_command->mutation_version = command.mutation_version;

    return *mutation_command;
}

AlterConversions::AlterConversions(
    const MutationCommands & mutation_commands_,
    const PatchPartsForReader & patch_parts_,
    const ContextPtr & context)
{
    for (const auto & command : mutation_commands_)
        addMutationCommand(command, context);

    for (const auto & patch : patch_parts_)
        addPatchPart(patch);

    /// Do not throw if there are no mutations or patches.
    if (number_of_alter_mutations > 1)
    {
        if (!mutation_commands.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Applying mutations on-fly is not supported with more than one ALTER MODIFY");

        if (!patch_parts.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Applying patch parts is not supported with more than one on-fly ALTER MODIFY");
    }
}

bool AlterConversions::hasLightweightDelete() const
{
    return all_updated_columns.contains(RowExistsColumn::name);
}

bool AlterConversions::hasDeleteMutation() const
{
    /// A lightweight DELETE arrives as a DELETE-typed command too, so this also covers it; the
    /// distinct point of this predicate is the ordinary ALTER DELETE, which adds nothing to
    /// all_updated_columns and does not set _row_exists.
    for (const auto & command : mutation_commands)
        if (command.type == MutationCommand::Type::DELETE)
            return true;
    return false;
}

bool AlterConversions::isSupportedDataMutation(MutationCommand::Type type)
{
    return type == MutationCommand::UPDATE || type == MutationCommand::DELETE;
}

bool AlterConversions::isSupportedAlterMutation(MutationCommand::Type type)
{
    return type == MutationCommand::READ_COLUMN;
}

bool AlterConversions::isSupportedMetadataMutation(MutationCommand::Type type)
{
    return type == MutationCommand::RENAME_COLUMN
        || type == MutationCommand::DROP_COLUMN;
}

void AlterConversions::addUpdatedColumns(const MutationCommand & command, NameSet & updated_columns)
{
    using enum MutationCommand::Type;

    if (command.type == READ_COLUMN)
    {
        /// This is needed to ignore skip indices that use the column as it's changing its type and no longer applies
        /// Note that data_type is only set on ADD_COLUMN and MODIFY_COLUMN commands
        if (command.data_type)
            updated_columns.insert(command.column_name);
    }
    else if (command.type == UPDATE || command.type == DELETE)
    {
        if (auto alter = command.ast(); alter && alter->update_assignments)
        {
            for (const auto & child : alter->update_assignments->children)
                updated_columns.insert(child->as<ASTAssignment &>().column_name);
        }
    }
}

void AlterConversions::addMutationCommand(const MutationCommand & command, const ContextPtr & context)
{
    using enum MutationCommand::Type;

    if (command.type == RENAME_COLUMN)
    {
        /// Handle chained renames: if column A was renamed to B, and now B is renamed to C,
        /// update the existing entry to map A directly to C instead of having two separate entries.
        bool chained = false;
        for (auto & entry : rename_map)
        {
            if (entry.rename_to == command.column_name)
            {
                entry.rename_to = command.rename_to;
                chained = true;
                break;
            }
        }
        if (!chained)
            rename_map.emplace_back(RenamePair{command.rename_to, command.column_name});
    }
    else if (command.type == DROP_COLUMN)
    {
        /// Handle a drop after a rename: if column A was renamed to B and B is now dropped,
        /// record the drop under A, and erase the mapping of A to B.
        auto dropped_column_name = command.column_name;
        auto it = std::ranges::find(rename_map, command.column_name, &RenamePair::rename_to);
        if (it != rename_map.end())
        {
            dropped_column_name = it->rename_from;
            rename_map.erase(it);
        }

        dropped_columns.emplace(std::move(dropped_column_name));
    }
    else if (command.type == READ_COLUMN)
    {
        ++number_of_alter_mutations;
        version_of_alter_mutation = command.mutation_version;
        addUpdatedColumns(command, all_updated_columns);
    }
    else if (command.type == UPDATE || command.type == DELETE)
    {
        const auto result = findFirstNonDeterministicFunction(command, context);
        if (result.subquery)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ALTER UPDATE/ALTER DELETE statement with subquery may be nondeterministic and cannot be applied on fly");

        if (result.nondeterministic_function_name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ALTER UPDATE/ALTER DELETE statements with nondeterministic deterministic functions cannot be applied on fly. "
                "Function '{}' is non-deterministic", *result.nondeterministic_function_name);

        addUpdatedColumns(command, all_updated_columns);
        mutation_commands.push_back(command);
    }
}

void AlterConversions::addPatchPart(PatchPartInfoForReader patch_part)
{
    /// Columns of the key the patch was written with must not be reported as updated columns.
    const auto & sorting_key_columns = patch_part.stored_sorting_key_columns;

    for (const auto & column : patch_part.part->getColumns())
    {
        if (isPatchPartSystemColumn(column.name) || sorting_key_columns.contains(column.name))
            continue;

        String updated_column_name = column.name;
        const auto & patch_conversions = patch_part.part->getAlterConversions();

        if (patch_conversions && patch_conversions->columnHasNewName(updated_column_name))
            updated_column_name = patch_conversions->getColumnNewName(column.name);

        all_updated_columns.insert(updated_column_name);
        columns_updated_in_patches.insert(updated_column_name);
    }

    /// For patches before ALTER MODIFY we should not apply conversions
    /// because correctness of ALTER MODIFY may depend on the data in patch part (the result of UPDATE).
    if (version_of_alter_mutation && !patchHasHigherDataVersion(*patch_part.part, *version_of_alter_mutation))
        patch_part.perform_alter_conversions = false;

    patch_parts.push_back(std::move(patch_part));
}

bool AlterConversions::columnHasNewName(const std::string & old_name) const
{
    for (const auto & [new_name, prev_name] : rename_map)
    {
        if (old_name == prev_name)
            return true;
    }

    return false;
}

std::string AlterConversions::getColumnNewName(const std::string & old_name) const
{
    for (const auto & [new_name, prev_name] : rename_map)
    {
        if (old_name == prev_name)
            return new_name;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} was not renamed", old_name);
}

bool AlterConversions::isColumnRenamed(const std::string & new_name) const
{
    for (const auto & [name_to, name_from] : rename_map)
    {
        if (name_to == new_name)
            return true;
    }
    return false;
}

/// Get column old name before rename (lookup by key in rename_map)
std::string AlterConversions::getColumnOldName(const std::string & new_name) const
{
    for (const auto & [name_to, name_from] : rename_map)
    {
        if (name_to == new_name)
            return name_from;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} was not renamed", new_name);
}

bool AlterConversions::isColumnDropped(const std::string & name, bool share_nested_offsets) const
{
    /// Check exact match (e.g. DROP COLUMN `n.s`)
    if (dropped_columns.contains(name))
        return true;

    /// When share_nested_offsets is disabled, dotted-name columns are independent
    /// and dropping `n` should not affect `n.a`.
    if (!share_nested_offsets)
        return false;

    /// Check if the parent nested column was dropped (e.g. DROP COLUMN `n` should match `n.s`, `n.d`, etc.)
    auto nested_prefix_end = name.find('.');
    if (nested_prefix_end != std::string::npos && dropped_columns.contains(name.substr(0, nested_prefix_end)))
        return true;

    return false;
}

PrewhereExprSteps AlterConversions::getMutationSteps(
    const IMergeTreeDataPartInfoForReader & part_info,
    const NamesAndTypesList & read_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context) const
{
    if (mutation_commands.empty())
        return {};

    const StorageID table_id = part_info.getDataPart()->storage.getStorageID();
    auto chain = buildMutationChainForRead(read_columns, metadata_snapshot, table_id, context);
    auto settings = ExpressionActionsSettings(context);

    /// Columns the surviving on-fly chain will overwrite. Attached to every
    /// pre-`MODIFY` step so `MergeTreeReadersChain::executeActionsBeforePrewhere`
    /// can skip `performRequiredConversions` for them: their on-disk value is
    /// about to be replaced and pre-casting it could fail on values the chain
    /// will discard (for example, `_CAST('x', UInt64)` before `UPDATE v = '100'`).
    ///
    /// The set is built from the chain that `filterMutationCommands` actually
    /// returns for this `read_columns`. Commands the query does not need are
    /// dropped there, otherwise an earlier surviving step that reads one of
    /// those columns as a source would see the on-disk type while the block
    /// already advertises the post-`MODIFY` type.
    ///
    /// `MutationActions::dag.getOutputs()` would give a superset (it lists
    /// passthrough columns too), so we read the assignment targets directly
    /// from the surviving commands.
    ///
    /// The skip is keyed on storage column names downstream
    /// (`MergeTreeReadersChain::executeActionsBeforePrewhere` calls
    /// `getNameInStorage()`). Assignment targets are top-level columns today;
    /// if per-subcolumn assignments to `Nested` columns ever become
    /// supported, the reader-side key has to switch accordingly.
    NameSet columns_overwritten_by_chain;
    for (const auto & command : chain.commands)
    {
        auto ast = command.ast();
        if (!ast)
        {
            continue;
        }
        if (command.type == MutationCommand::UPDATE)
        {
            for (const auto & [column, _] : getColumnToUpdateExpression(*ast))
            {
                columns_overwritten_by_chain.insert(column);
            }
        }
        else if (command.type == MutationCommand::DELETE)
        {
            /// Inserted for any chained `DELETE`. Lightweight delete
            /// arrives as a `DELETE`-typed command without the original
            /// `_row_exists = 0` assignment, so the explicit insert is
            /// the only way to keep it skipped. Plain `ALTER DELETE` does
            /// not have an on-disk `_row_exists`, so the insert is a
            /// no-op for `performRequiredConversions`.
            columns_overwritten_by_chain.insert(RowExistsColumn::name);
        }
    }

    auto actions_chain = getMutationActions(part_info, std::move(chain), metadata_snapshot, context);

    /// A mutation expression is evaluated with the sample factor its background materialization sees,
    /// which is 1 (`createMergeTreeSequentialSource`), never the reading query's factor. Only the
    /// expression's dependency is rebound; the column the step forwards to the query is unaffected.
    const bool bind_sample_factor = metadata_snapshot->isVirtualColumn("_sample_factor");

    PrewhereExprSteps steps;
    for (auto & actions : actions_chain)
    {
        if (bind_sample_factor)
        {
            DataTypePtr type = std::make_shared<DataTypeFloat64>();
            auto column = type->createColumnConst(1, Field(1.0));
            actions.dag.substituteInputForConsumersOnly(
                "_sample_factor", ColumnWithTypeAndName{column->getPtr(), type, "_sample_factor"});
        }

        /// For mutations before ALTER MODIFY we should not apply conversions
        /// because correctness of ALTER MODIFY may depend on the result of mutation.
        bool perform_alter_conversions = !version_of_alter_mutation || actions.mutation_version > version_of_alter_mutation;
        bool is_filter = !actions.filter_column_name.empty();

        PrewhereExprStep step
        {
            .type = is_filter ? PrewhereExprStep::Filter : PrewhereExprStep::Expression,
            .actions = std::make_shared<ExpressionActions>(std::move(actions.dag), settings, actions.project_input),
            .filter_column_name = actions.filter_column_name,
            .remove_filter_column = false,
            .need_filter = is_filter,
            .perform_alter_conversions = perform_alter_conversions,
            .columns_overwritten_by_chain = perform_alter_conversions ? NameSet{} : columns_overwritten_by_chain,
            .mutation_version = actions.mutation_version,
        };

        steps.push_back(std::make_shared<PrewhereExprStep>(std::move(step)));
    }

    return steps;
}

PatchPartsForReader AlterConversions::getPatchesForColumns(const NamesAndTypesList & read_columns, bool apply_deleted_mask) const
{
    PatchPartsForReader patches_to_read;

    size_t num_join = 0;
    size_t num_merge = 0;
    size_t num_merge_on_key = 0;

    for (const auto & patch : patch_parts)
    {
        bool has_column_in_patch = false;
        const auto & patch_conversions = patch.part->getAlterConversions();

        /// If patch has lightweight delete we have to always apply it.
        if (apply_deleted_mask && patch.part->hasLightweightDelete())
        {
            has_column_in_patch = true;
        }
        else
        {
            /// Columns of the key the patch was written with must not be reported as updated columns.
            const auto & sorting_key_columns = patch.stored_sorting_key_columns;

            has_column_in_patch = std::ranges::any_of(read_columns, [&](const auto & column)
            {
                if (isPatchPartSystemColumn(column.name))
                    return false;

                auto name_in_storage = column.getNameInStorage();

                if (patch_conversions && patch_conversions->isColumnRenamed(name_in_storage))
                    name_in_storage = patch_conversions->getColumnOldName(name_in_storage);

                if (sorting_key_columns.contains(name_in_storage))
                    return false;

                return patch.part->getColumnsDescription().hasPhysical(name_in_storage);
            });
        }

        if (has_column_in_patch)
        {
            switch (patch.mode)
            {
                case PatchMode::Join:       ++num_join; break;
                case PatchMode::Merge:      ++num_merge; break;
                case PatchMode::MergeOnKey: ++num_merge_on_key; break;
            }

            patches_to_read.push_back(patch);
        }
    }

    if (!patches_to_read.empty())
    {
        ProfileEvents::increment(ProfileEvents::ReadTasksWithAppliedPatches);
        ProfileEvents::increment(ProfileEvents::PatchesAppliedInAllReadTasks, patches_to_read.size());
        ProfileEvents::increment(ProfileEvents::PatchesJoinAppliedInAllReadTasks, num_join);
        ProfileEvents::increment(ProfileEvents::PatchesMergeAppliedInAllReadTasks, num_merge);
        ProfileEvents::increment(ProfileEvents::PatchesMergeOnKeyAppliedInAllReadTasks, num_merge_on_key);
    }

    return patches_to_read;
}

/// Extends the read set with the columns needed to recalculate the MATERIALIZED columns it contains.
/// A MATERIALIZED column is stored, so a query selecting it does not ask for what its expression reads,
/// yet the read set gates which commands survive `filterMutationCommands` — a missing dependency leaves
/// the pending mutation unapplied for this read task and returns the stale stored value.
static void addColumnsRequiredForMaterialized(
    Names & read_columns,
    NameSet & read_columns_set,
    const MaterializedColumnDependencies & dependencies,
    const NameSet & updated_columns)
{
    std::queue<String> columns_to_visit(read_columns_set.begin(), read_columns_set.end());

    while (!columns_to_visit.empty())
    {
        auto column_name = std::move(columns_to_visit.front());
        columns_to_visit.pop();

        /// Asked per column rather than as a precomputed set, so only the reachable part of the
        /// graph is analysed.
        for (const auto & dependency : dependencies.findColumnsToRecalculate(column_name, updated_columns))
        {
            if (read_columns_set.emplace(dependency).second)
            {
                read_columns.push_back(dependency);
                columns_to_visit.push(dependency);
            }
        }
    }
}

AlterConversions::MutationChainForRead AlterConversions::buildMutationChainForRead(
    const NamesAndTypesList & read_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const StorageID & table_id,
    const ContextPtr & context) const
{
    MutationChainForRead chain;
    NameSet read_columns_set;

    for (const auto & column : read_columns)
    {
        auto name_in_storage = column.getNameInStorage();
        if (read_columns_set.emplace(name_in_storage).second)
            chain.read_columns.emplace_back(name_in_storage);
    }

    MaterializedColumnDependencies dependencies(metadata_snapshot->getColumns(), context);

    /// Adding what a MATERIALIZED recalculation needs and filtering the commands feed each other: the
    /// filter keeps a command only if it touches the read set, and adds what that command reads —
    /// possibly a MATERIALIZED column (`DELETE WHERE m2 > 100`) whose own dependencies the next round
    /// adds in turn.
    size_t old_read_columns_size = 0;
    do
    {
        old_read_columns_size = chain.read_columns.size();
        addColumnsRequiredForMaterialized(chain.read_columns, read_columns_set, dependencies, all_updated_columns);
        chain.commands = filterMutationCommands(chain.read_columns, read_columns_set, metadata_snapshot, table_id, context);
    } while (old_read_columns_size != chain.read_columns.size());

    return chain;
}

std::vector<MutationActions> AlterConversions::getMutationActions(
    const IMergeTreeDataPartInfoForReader & part_info,
    MutationChainForRead chain,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context) const
{
    const auto * loaded_part_info = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(&part_info);
    if (!loaded_part_info)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Applying mutations on-fly is supported only for loaded data parts");

    auto & filtered_commands = chain.commands;
    if (filtered_commands.empty())
        return {};

    ProfileEvents::increment(ProfileEvents::ReadTasksWithAppliedMutationsOnFly);
    ProfileEvents::increment(ProfileEvents::MutationsAppliedOnFlyInAllReadTasks, filtered_commands.size());

    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.recalculate_dependencies_of_updated_columns = false;
    settings.apply_on_fly_for_read = true;

    const auto & part = loaded_part_info->getDataPart();
    auto alter_conversions = std::make_shared<AlterConversions>();

    MutationsInterpreter interpreter(
        const_cast<MergeTreeData &>(part->storage),
        part,
        alter_conversions,
        metadata_snapshot,
        std::move(filtered_commands),
        std::move(chain.read_columns),
        context,
        settings);

    return interpreter.getMutationActions();
}

namespace
{

/// Collects the storage columns, physical or virtual, that the identifiers of an expression over the table may read.
/// An ALIAS column reads what its definition reads, a flattened Nested group its columns. A lambda parameter reads nothing
/// inside its lambda, and a name that resolves to none of these, such as an alias defined in the expression, reads nothing.
class ColumnsReadByExpressionCollector
{
public:
    ColumnsReadByExpressionCollector(const StorageInMemoryMetadata & metadata_, const StorageID & table_id_, const ContextPtr & context_)
        : metadata(metadata_)
        , table_id(table_id_)
        , context(context_)
        , prefer_name_after_table_qualifier(context_->getSettingsRef()[Setting::analyzer_compatibility_prefer_alias_over_subcolumn])
        , allow_compound_nested_members(
              context_->getSettingsRef()[Setting::analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested])
    {
    }

    void collect(const IAST & ast, NameOrderedSet & columns)
    {
        if (const auto * function = ast.as<ASTFunction>();
            function && function->name == "lambda" && function->arguments && function->arguments->children.size() == 2)
        {
            const auto & arguments = function->arguments->children;
            const size_t enclosing_parameters = lambda_parameters.size();
            if (const auto * parameters = arguments[0]->as<ASTFunction>(); parameters && parameters->arguments)
            {
                for (const auto & parameter : parameters->arguments->children)
                    if (const auto * parameter_identifier = parameter->as<ASTIdentifier>())
                        lambda_parameters.push_back(parameter_identifier->name());
            }
            else if (const auto * parameter_identifier = arguments[0]->as<ASTIdentifier>())
            {
                lambda_parameters.push_back(parameter_identifier->name());
            }

            collect(*arguments[1], columns);
            lambda_parameters.resize(enclosing_parameters);
            return;
        }

        if (const auto * identifier = ast.as<ASTIdentifier>())
        {
            /// Inside a lambda body a name bound by it is the parameter, or a subcolumn of it, even if a column of that name exists.
            const auto & name_parts = identifier->name_parts;
            if (name_parts.empty() || std::ranges::find(lambda_parameters, name_parts.front()) != lambda_parameters.end())
                return;

            /// A name reads the columns of the one interpretation the analyzer binds, in its order: the whole name (first the name after
            /// `table.` with `analyzer_compatibility_prefer_alias_over_subcolumn`), then after `table.`, then after `database.table.`.
            const bool qualified_by_table = name_parts.size() > 1 && name_parts[0] == table_id.table_name;
            if (qualified_by_table && prefer_name_after_table_qualifier && addNameAfterQualifier(name_parts, 1, columns))
                return;
            if (addNameAfterQualifier(name_parts, 0, columns))
                return;
            if (qualified_by_table && addNameAfterQualifier(name_parts, 1, columns))
                return;
            if (name_parts.size() > 2 && name_parts[0] == table_id.database_name && name_parts[1] == table_id.table_name)
                addNameAfterQualifier(name_parts, 2, columns);
            return;
        }

        for (const auto & child : ast.children)
            collect(*child, columns);
    }

private:
    bool addNameAfterQualifier(const std::vector<String> & name_parts, size_t qualifier_parts, NameOrderedSet & columns)
    {
        String name;
        for (size_t i = qualifier_parts; i < name_parts.size(); ++i)
        {
            if (i > qualifier_parts)
                name += '.';
            name += name_parts[i];
        }
        if (addCandidate(name, columns))
            return true;

        /// The shortest ALIAS prefix whose type has the rest as a subcolumn, as the analyzer binds it.
        const auto & columns_desc = metadata.getColumns();
        String prefix_name;
        for (size_t i = qualifier_parts; i + 1 < name_parts.size(); ++i)
        {
            if (i > qualifier_parts)
                prefix_name += '.';
            prefix_name += name_parts[i];

            const auto * prefix = columns_desc.tryGet(prefix_name);
            if (prefix && prefix->default_desc.kind == ColumnDefaultKind::Alias
                && prefix->type->tryGetSubcolumnType(std::string_view(name).substr(prefix_name.size() + 1)))
            {
                addColumnOrSubcolumn(prefix_name, columns);
                return true;
            }
        }

        if (name_parts.size() - qualifier_parts != 1)
            return false;

        /// A flattened Nested group is the Array columns `name.<suffix>` of the table.
        const String member_prefix = name + '.';
        bool found_member = false;
        for (const auto & column : columns_desc)
        {
            if (!column.name.starts_with(member_prefix) || !typeid_cast<const DataTypeArray *>(column.type.get()))
                continue;
            if (!allow_compound_nested_members && column.name.find('.', member_prefix.size()) != String::npos)
                continue;
            addColumnOrSubcolumn(column.name, columns);
            found_member = true;
        }
        return found_member;
    }

    bool addCandidate(const String & name, NameOrderedSet & columns)
    {
        if (addColumnOrSubcolumn(name, columns))
            return true;

        if (metadata.virtuals.has(name))
        {
            columns.insert(name);
            return true;
        }

        return false;
    }

    bool addColumnOrSubcolumn(const String & name, NameOrderedSet & columns)
    {
        const auto & columns_desc = metadata.getColumns();
        auto column = columns_desc.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
        if (!column)
            return false;

        auto name_in_storage = column->getNameInStorage();
        const auto * description = columns_desc.tryGet(name_in_storage);
        if (!description || description->default_desc.kind != ColumnDefaultKind::Alias)
        {
            columns.insert(name_in_storage);
            return true;
        }

        if (!description->default_desc.expression || !followed_aliases.emplace(name_in_storage).second)
            return true;

        const auto nondeterministic = findFirstNonDeterministicFunction(description->default_desc.expression, context);
        if (nondeterministic.nondeterministic_function_name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ALTER UPDATE/ALTER DELETE statement reads ALIAS column {} defined with non-deterministic function '{}' "
                "and cannot be applied on fly",
                name_in_storage, *nondeterministic.nondeterministic_function_name);

        /// A definition is authored at table scope, so the enclosing lambdas do not bind its names.
        std::vector<String> enclosing_lambda_parameters;
        lambda_parameters.swap(enclosing_lambda_parameters);
        collect(*description->default_desc.expression, columns);
        lambda_parameters.swap(enclosing_lambda_parameters);
        return true;
    }

    const StorageInMemoryMetadata & metadata;
    const StorageID & table_id;
    ContextPtr context;
    bool prefer_name_after_table_qualifier;
    bool allow_compound_nested_members;
    /// Each definition is walked once, which bounds diamond-shaped chains of aliases.
    NameSet followed_aliases;
    std::vector<String> lambda_parameters;
};

}

MutationCommands AlterConversions::filterMutationCommands(
    Names & read_columns,
    NameSet & read_columns_set,
    const StorageMetadataPtr & metadata_snapshot,
    const StorageID & table_id,
    const ContextPtr & context) const
{
    /// The read set is keyed on storage columns, so a command contributes the storage columns its identifiers read.
    ColumnsReadByExpressionCollector collector(*metadata_snapshot, table_id, context);
    MutationCommands filtered_commands;

    /// We need to read all columns that are used in mutation.
    /// Therefore we need to add all previous mutations that affects such columns.
    /// Because of that we iterate over commands backwards.
    for (const auto & command : mutation_commands | std::views::reverse)
    {
        NameOrderedSet source_columns;
        if (command.type == MutationCommand::Type::DELETE)
        {
            collector.collect(*command.ast()->predicate, source_columns);
            filtered_commands.push_back(command);
        }
        else if (command.type == MutationCommand::Type::UPDATE)
        {
            bool has_lightweight_delete = false;
            std::unordered_map<String, ASTPtr> new_updated_columns;

            auto alter = command.ast();
            if (alter && alter->update_assignments)
            {
                for (const auto & child : alter->update_assignments->children)
                {
                    const auto & assignment = child->as<ASTAssignment &>();
                    auto expr = assignment.expression();
                    if (isLightweightDeleteCommand(assignment.column_name, expr))
                    {
                        has_lightweight_delete = true;
                    }
                    else if (read_columns_set.contains(assignment.column_name))
                    {
                        collector.collect(*expr, source_columns);
                        new_updated_columns.emplace(assignment.column_name, expr->clone());
                    }
                }
            }

            if (has_lightweight_delete)
            {
                auto new_command = createLightweightDeleteCommand(command);
                collector.collect(*new_command.ast()->predicate, source_columns);
                filtered_commands.push_back(std::move(new_command));
            }

            if (!new_updated_columns.empty())
            {
                auto new_command = createCommandWithUpdatedColumns(command, new_updated_columns);
                collector.collect(*new_command.ast()->predicate, source_columns);
                filtered_commands.push_back(std::move(new_command));
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected mutation of type {} in AlterConversions. Only UPDATE and DELETE mutations are supported",
                magic_enum::enum_name(command.type));
        }

        for (const auto & column : source_columns)
            if (read_columns_set.emplace(column).second)
                read_columns.push_back(column);
    }

    std::reverse(filtered_commands.begin(), filtered_commands.end());
    return filtered_commands;
}

void MutationCounters::assertNotNegative() const
{
    if (num_data < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly data mutations counter is negative ({})", num_data);

    if (num_alter < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly alter mutations counter is negative ({})", num_alter);

    if (num_metadata < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "On-fly metadata mutations counter is negative ({})", num_metadata);
}

}
