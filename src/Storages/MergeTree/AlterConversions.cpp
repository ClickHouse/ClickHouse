#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTLiteral.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
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

    alter_command->predicate = alter_command->children.emplace_back(src_alter->predicate->clone()).get();
    auto mutation_command = MutationCommand::parse(
        *alter_command,
        /* parse_alter_commands = */ false,
        /* with_pure_metadata_commands = */ false,
        command.max_parser_depth,
        command.max_parser_backtracks);

    if (!mutation_command)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse command {}", alter_command->formatForErrorMessage());

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
        dropped_columns.emplace(command.column_name);
    }
    else if (command.type == READ_COLUMN)
    {
        ++number_of_alter_mutations;
        version_of_alter_mutation = command.mutation_version;

        /// This is needed to ignore skip indices that use the column as it's changing its type and no longer applies
        /// Note that data_type is only set on ADD_COLUMN and MODIFY_COLUMN commands
        if (command.data_type)
            all_updated_columns.insert(command.column_name);
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

        if (auto alter = command.ast(); alter && alter->update_assignments)
        {
            for (const auto & child : alter->update_assignments->children)
                all_updated_columns.insert(child->as<ASTAssignment &>().column_name);
        }

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
    auto actions_chain = getMutationActions(part_info, read_columns, metadata_snapshot, context);
    auto settings = ExpressionActionsSettings(context);

    /// Columns the surviving on-fly chain will overwrite. Attached to every
    /// pre-`MODIFY` step so `MergeTreeReadersChain::executeActionsBeforePrewhere`
    /// can skip `performRequiredConversions` for them: their on-disk value is
    /// about to be replaced and pre-casting it could fail on values the chain
    /// will discard (for example, `_CAST('x', UInt64)` before `UPDATE v = '100'`).
    ///
    /// The set is built from the chain that `filterMutationCommands` actually
    /// returns for this `read_columns`. Commands the query does not need are
    /// dropped here, otherwise an earlier surviving step that reads one of
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
    if (!actions_chain.empty())
    {
        Names storage_read_columns;
        NameSet storage_read_columns_set;
        for (const auto & column : read_columns)
        {
            auto name_in_storage = column.getNameInStorage();
            if (storage_read_columns_set.emplace(name_in_storage).second)
            {
                storage_read_columns.emplace_back(name_in_storage);
            }
        }
        addColumnsRequiredForMaterialized(storage_read_columns, storage_read_columns_set, metadata_snapshot, context);
        for (const auto & command : filterMutationCommands(storage_read_columns, std::move(storage_read_columns_set)))
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
    }

    PrewhereExprSteps steps;
    for (auto & actions : actions_chain)
    {
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

std::vector<MutationActions> AlterConversions::getMutationActions(
    const IMergeTreeDataPartInfoForReader & part_info,
    const NamesAndTypesList & read_columns,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context) const
{
    if (mutation_commands.empty())
        return {};

    const auto * loaded_part_info = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(&part_info);
    if (!loaded_part_info)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Applying mutations on-fly is supported only for loaded data parts");

    Names storage_read_columns;
    NameSet storage_read_columns_set;

    for (const auto & column : read_columns)
    {
        auto name_in_storage = column.getNameInStorage();
        if (storage_read_columns_set.emplace(name_in_storage).second)
            storage_read_columns.emplace_back(name_in_storage);
    }

    addColumnsRequiredForMaterialized(storage_read_columns, storage_read_columns_set, metadata_snapshot, context);
    auto filtered_commands = filterMutationCommands(storage_read_columns, std::move(storage_read_columns_set));

    if (filtered_commands.empty())
        return {};

    ProfileEvents::increment(ProfileEvents::ReadTasksWithAppliedMutationsOnFly);
    ProfileEvents::increment(ProfileEvents::MutationsAppliedOnFlyInAllReadTasks, filtered_commands.size());

    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.recalculate_dependencies_of_updated_columns = false;

    const auto & part = loaded_part_info->getDataPart();
    auto alter_conversions = std::make_shared<AlterConversions>();

    MutationsInterpreter interpreter(
        const_cast<MergeTreeData &>(part->storage),
        part,
        alter_conversions,
        metadata_snapshot,
        std::move(filtered_commands),
        std::move(storage_read_columns),
        context,
        settings);

    return interpreter.getMutationActions();
}

/// Extends the read set with the columns needed to recalculate the MATERIALIZED columns it contains.
///
/// A MATERIALIZED column is stored, so a query selecting it does not ask for the columns its expression
/// reads. The read set must be closed over them: it gates which commands survive `filterMutationCommands`
/// and which columns the interpreter can resolve expressions against, so a missing dependency leaves the
/// pending mutation unapplied for this read task and returns the stale stored value.
///
/// Three rules govern the walk:
/// 1. Transitive: the updated column may be reachable only through other MATERIALIZED columns
///    (`m2 MATERIALIZED m1 + 1` over `m1 MATERIALIZED x + 1` with `x` updated), so the MATERIALIZED
///    columns in between are read as well.
/// 2. Reachability-gated: a dependency is added only when an updated column is reachable through it,
///    so an unrelated chain is neither read nor keeps a command the query does not need alive.
/// 3. EPHEMERAL-aware: a column reading an EPHEMERAL one is never recalculated outside INSERT, so it is
///    not read to complete a chain passing through it and keeps its stored value. An EPHEMERAL column
///    is not readable and appears in the analysis only so that such an expression resolves.
void AlterConversions::addColumnsRequiredForMaterialized(
    Names & read_columns,
    NameSet & read_columns_set,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context) const
{
    const auto & columns_desc = metadata_snapshot->getColumns();
    auto source_columns = columns_desc.getAllPhysical();

    /// `getAllPhysical` omits EPHEMERAL columns, but a MATERIALIZED expression may mention one and
    /// analysing it without them fails with UNKNOWN_IDENTIFIER.
    NameSet ephemeral_columns;
    for (const auto & column : columns_desc.getEphemeral())
    {
        ephemeral_columns.insert(column.name);
        source_columns.push_back(column);
    }

    /// `tryGet`, because the read set also holds virtual columns, which `columns_desc` does not know.
    auto is_materialized = [&](const String & column_name)
    {
        const auto * column = columns_desc.tryGet(column_name);
        return column && column->default_desc.kind == ColumnDefaultKind::Materialized && column->default_desc.expression;
    };

    std::unordered_map<String, Names> dependencies_of;
    auto get_dependencies = [&](const String & column_name) -> const Names &
    {
        auto it = dependencies_of.find(column_name);
        if (it != dependencies_of.end())
            return it->second;

        /// Cloned because both calls below rewrite the expression in place, and it belongs to the shared
        /// metadata snapshot. Only reached for a column `is_materialized` accepted, so `get` cannot throw.
        auto query = columns_desc.get(column_name).default_desc.expression->clone();
        /// Must match `MutationsInterpreter::prepare`: without the rewrite a default over a subcolumn
        /// depends on `t.a` while the updated column is named `t`, so the dependency goes unrecognised.
        replaceSubcolumnsToGetSubcolumnFunctionInQuery(query, source_columns);
        auto syntax_result = TreeRewriter(context).analyze(query, source_columns);
        return dependencies_of.emplace(column_name, syntax_result->requiredSourceColumns()).first->second;
    };

    /// Whether a dependency is a MATERIALIZED column the interpreter will actually recalculate, and so
    /// may stand in the middle of a chain.
    auto can_recalculate = [&](const String & column_name)
    {
        if (!is_materialized(column_name))
            return false;

        const auto & dependencies = get_dependencies(column_name);
        return std::ranges::none_of(dependencies, [&](const auto & dep) { return ephemeral_columns.contains(dep); });
    };

    /// Whether a column has to be read: it is updated itself, or it is a MATERIALIZED column that the
    /// interpreter recalculates from an updated column further down the chain. Memoised, and the entry
    /// inserted before recursing also breaks a cycle, which well-formed defaults cannot contain; on an
    /// acyclic graph an entry is only ever read once its value is final, because a column still being
    /// visited is reachable exclusively from its own subtree.
    std::unordered_map<String, bool> needs_reading_of;
    auto needs_reading = [&](const String & column_name, auto && self) -> bool
    {
        if (all_updated_columns.contains(column_name))
            return true;

        if (!can_recalculate(column_name))
            return false;

        auto [it, inserted] = needs_reading_of.emplace(column_name, false);
        if (!inserted)
            return it->second;

        for (const auto & dependency : get_dependencies(column_name))
        {
            if (self(dependency, self))
            {
                /// Not through `it`: the recursion above may have rehashed the map.
                needs_reading_of[column_name] = true;
                return true;
            }
        }

        return false;
    };

    std::queue<String> columns_to_visit(read_columns_set.begin(), read_columns_set.end());

    while (!columns_to_visit.empty())
    {
        auto column_name = std::move(columns_to_visit.front());
        columns_to_visit.pop();

        /// Only a column the interpreter recalculates needs its dependencies in the read set, and it
        /// needs all of them, because the stage that rewrites it evaluates its whole expression against
        /// the block — including the parts that read a column no mutation touches. A column that keeps
        /// its stored value needs nothing, and one that cannot be recalculated at all (a plain or virtual
        /// column, or a MATERIALIZED column reading an EPHEMERAL one) is not rewritten either.
        if (!can_recalculate(column_name) || !needs_reading(column_name, needs_reading))
            continue;

        /// `can_recalculate` held, so none of these is an EPHEMERAL column.
        for (const auto & dependency : get_dependencies(column_name))
        {
            if (read_columns_set.contains(dependency))
                continue;

            read_columns_set.insert(dependency);
            read_columns.push_back(dependency);
            columns_to_visit.push(dependency);
        }
    }
}

MutationCommands AlterConversions::filterMutationCommands(Names & read_columns, NameSet read_columns_set) const
{
    MutationCommands filtered_commands;

    /// We need to read all columns that are used in mutation.
    /// Therefore we need to add all previous mutations that affects such columns.
    /// Because of that we iterate over commands backwards.
    for (const auto & command : mutation_commands | std::views::reverse)
    {
        IdentifierNameSet source_columns;
        if (command.type == MutationCommand::Type::DELETE)
        {
            command.ast()->predicate->collectIdentifierNames(source_columns);
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
                        expr->collectIdentifierNames(source_columns);
                        new_updated_columns.emplace(assignment.column_name, expr->clone());
                    }
                }
            }

            if (has_lightweight_delete)
            {
                auto new_command = createLightweightDeleteCommand(command);
                new_command.ast()->predicate->collectIdentifierNames(source_columns);
                filtered_commands.push_back(std::move(new_command));
            }

            if (!new_updated_columns.empty())
            {
                auto new_command = createCommandWithUpdatedColumns(command, new_updated_columns);
                new_command.ast()->predicate->collectIdentifierNames(source_columns);
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
        {
            if (read_columns_set.emplace(column).second)
                read_columns.push_back(column);
        }
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
