#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/StorageView.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/typeid_cast.h>
#include <Common/randomSeed.h>

#include <ranges>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_codecs;
    extern const SettingsBool allow_suspicious_codecs;
    extern const SettingsBool allow_suspicious_ttl_expressions;
    extern const SettingsBool enable_zstd_qat_codec;
    extern const SettingsBool flatten_nested;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_STATISTICS;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

AlterCommand::RemoveProperty removePropertyFromString(const String & property)
{
    if (property.empty())
        return AlterCommand::RemoveProperty::NO_PROPERTY;
    if (property == "DEFAULT")
        return AlterCommand::RemoveProperty::DEFAULT;
    if (property == "MATERIALIZED")
        return AlterCommand::RemoveProperty::MATERIALIZED;
    if (property == "ALIAS")
        return AlterCommand::RemoveProperty::ALIAS;
    if (property == "COMMENT")
        return AlterCommand::RemoveProperty::COMMENT;
    if (property == "CODEC")
        return AlterCommand::RemoveProperty::CODEC;
    if (property == "TTL")
        return AlterCommand::RemoveProperty::TTL;
    if (property == "SETTINGS")
        return AlterCommand::RemoveProperty::SETTINGS;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove unknown property '{}'", property);
}

}

std::optional<AlterCommand> AlterCommand::parse(const ASTAlterCommand * command_ast)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    if (command_ast->type == ASTAlterCommand::ADD_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::ADD_COLUMN;

        const auto & ast_col_decl = command_ast->col_decl->as<ASTColumnDeclaration &>();

        command.column_name = ast_col_decl.name;
        if (ast_col_decl.type)
        {
            command.data_type = data_type_factory.get(ast_col_decl.type);
        }
        if (ast_col_decl.default_expression)
        {
            command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
            command.default_expression = ast_col_decl.default_expression;
        }

        if (ast_col_decl.comment)
        {
            const auto & ast_comment = typeid_cast<ASTLiteral &>(*ast_col_decl.comment);
            command.comment = ast_comment.value.safeGet<String>();
        }

        if (ast_col_decl.codec)
        {
            if (ast_col_decl.default_specifier == "ALIAS")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
            command.codec = ast_col_decl.codec;
        }
        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        command.first = command_ast->first;
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_column)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_COLUMN;

        const auto & ast_col_decl = command_ast->col_decl->as<ASTColumnDeclaration &>();
        command.column_name = ast_col_decl.name;
        command.to_remove = removePropertyFromString(command_ast->remove_property);

        if (ast_col_decl.type)
        {
            command.data_type = data_type_factory.get(ast_col_decl.type);
        }

        if (ast_col_decl.default_expression)
        {
            command.default_kind = columnDefaultKindFromString(ast_col_decl.default_specifier);
            command.default_expression = ast_col_decl.default_expression;
        }

        if (ast_col_decl.comment)
        {
            const auto & ast_comment = ast_col_decl.comment->as<ASTLiteral &>();
            command.comment.emplace(ast_comment.value.safeGet<String>());
        }

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        if (ast_col_decl.codec)
            command.codec = ast_col_decl.codec;

        if (ast_col_decl.settings)
            command.settings_changes = ast_col_decl.settings->as<ASTSetQuery &>().changes;

        /// At most only one of ast_col_decl.settings or command_ast->settings_changes is non-null
        if (command_ast->settings_changes)
        {
            command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
            command.append_column_setting = true;
        }

        if (command_ast->settings_resets)
        {
            for (const ASTPtr & identifier_ast : command_ast->settings_resets->children)
            {
                const auto & identifier = identifier_ast->as<ASTIdentifier &>();
                command.settings_resets.emplace(identifier.name());
            }
        }

        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        command.first = command_ast->first;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::COMMENT_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.safeGet<String>();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_COMMENT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_TABLE;
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.safeGet<String>();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_ORDER_BY;
        command.order_by = command_ast->order_by->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SAMPLE_BY;
        command.sample_by = command_ast->sample_by->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::REMOVE_SAMPLE_BY;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.index_decl = command_ast->index_decl->clone();
        command.type = AlterCommand::ADD_INDEX;

        const auto & ast_index_decl = command_ast->index_decl->as<ASTIndexDeclaration &>();

        command.index_name = ast_index_decl.name;

        if (command_ast->index)
            command.after_index_name = command_ast->index->as<ASTIdentifier &>().name();

        command.if_not_exists = command_ast->if_not_exists;
        command.first = command_ast->first;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_STATISTICS)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.statistics_decl = command_ast->statistics_decl->clone();
        command.type = AlterCommand::ADD_STATISTICS;

        const auto & ast_stat_decl = command_ast->statistics_decl->as<ASTStatisticsDeclaration &>();

        command.statistics_columns = ast_stat_decl.getColumnNames();
        command.statistics_types = ast_stat_decl.getTypeNames();
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_STATISTICS)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.statistics_decl = command_ast->statistics_decl->clone();
        command.type = AlterCommand::MODIFY_STATISTICS;

        const auto & ast_stat_decl = command_ast->statistics_decl->as<ASTStatisticsDeclaration &>();

        command.statistics_columns = ast_stat_decl.getColumnNames();
        command.statistics_types = ast_stat_decl.getTypeNames();
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.constraint_decl = command_ast->constraint_decl->clone();
        command.type = AlterCommand::ADD_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::ADD_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.projection_decl = command_ast->projection_decl->clone();
        command.type = AlterCommand::ADD_PROJECTION;

        const auto & ast_projection_decl = command_ast->projection_decl->as<ASTProjectionDeclaration &>();

        command.projection_name = ast_projection_decl.name;

        if (command_ast->projection)
            command.after_projection_name = command_ast->projection->as<ASTIdentifier &>().name();

        command.first = command_ast->first;
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.if_exists = command_ast->if_exists;
        command.type = AlterCommand::DROP_CONSTRAINT;
        command.constraint_name = command_ast->constraint->as<ASTIdentifier &>().name();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = command_ast->index->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_index)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_STATISTICS)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.statistics_decl = command_ast->statistics_decl->clone();
        command.type = AlterCommand::DROP_STATISTICS;
        const auto & ast_stat_decl = command_ast->statistics_decl->as<ASTStatisticsDeclaration &>();

        command.statistics_columns = ast_stat_decl.getColumnNames();
        command.if_exists = command_ast->if_exists;
        command.clear = command_ast->clear_statistics;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::DROP_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_PROJECTION;
        command.projection_name = command_ast->projection->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_projection)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition->clone();

        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_TTL;
        command.ttl = command_ast->ttl->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::REMOVE_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::REMOVE_TTL;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_DATABASE_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::RESET_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RESET_SETTING;
        for (const ASTPtr & identifier_ast : command_ast->settings_resets->children)
        {
            const auto & identifier = identifier_ast->as<ASTIdentifier &>();
            auto insertion = command.settings_resets.emplace(identifier.name());
            if (!insertion.second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate setting name {}", backQuote(identifier.name()));
        }
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_QUERY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_QUERY;
        command.select = command_ast->select->clone();
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_REFRESH)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_REFRESH;
        command.refresh = command_ast->refresh;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::RENAME_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RENAME_COLUMN;
        command.column_name = command_ast->column->as<ASTIdentifier &>().name();
        command.rename_to = command_ast->rename_to->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    if (command_ast->type == ASTAlterCommand::MODIFY_SQL_SECURITY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SQL_SECURITY;
        command.sql_security = command_ast->sql_security->clone();
        return command;
    }

    return {};
}


void AlterCommand::apply(StorageInMemoryMetadata & metadata, ContextPtr context) const
{
    if (type == ADD_COLUMN)
    {
        ColumnDescription column(column_name, data_type);
        if (default_expression)
        {
            column.default_desc.kind = default_kind;
            column.default_desc.expression = default_expression;
        }
        if (comment)
            column.comment = *comment;

        if (codec)
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, data_type, false, true, true);

        column.ttl = ttl;

        if (context->getSettingsRef()[Setting::flatten_nested])
        {
            StorageInMemoryMetadata temporary_metadata;
            temporary_metadata.columns.add(column, /*after_column*/ "", /*first*/ true);
            temporary_metadata.columns.flattenNested();

            const auto transformed_columns = temporary_metadata.columns.getAll();

            auto add_column = [&](const String & name)
            {
                const auto & transformed_column = temporary_metadata.columns.get(name);
                metadata.columns.add(transformed_column, after_column, first);
            };

            if (!after_column.empty() || first)
            {
                for (const auto & col: transformed_columns | std::views::reverse)
                    add_column(col.name);
            }
            else
            {
                for (const auto & col: transformed_columns)
                    add_column(col.name);
            }
        }
        else
        {
            metadata.columns.add(column, after_column, first);
        }
    }
    else if (type == DROP_COLUMN)
    {
        /// Otherwise just clear data on disk
        if (!clear && !partition)
            metadata.columns.remove(column_name);
    }
    else if (type == MODIFY_COLUMN)
    {
        metadata.columns.modify(column_name, after_column, first, [&](ColumnDescription & column)
        {
            if (to_remove == RemoveProperty::DEFAULT
                || to_remove == RemoveProperty::MATERIALIZED
                || to_remove == RemoveProperty::ALIAS)
            {
                column.default_desc = ColumnDefault{};
            }
            else if (to_remove == RemoveProperty::CODEC)
            {
                column.codec.reset();
            }
            else if (to_remove == RemoveProperty::COMMENT)
            {
                column.comment = String{};
            }
            else if (to_remove == RemoveProperty::TTL)
            {
                column.ttl.reset();
            }
            else if (to_remove == RemoveProperty::SETTINGS)
            {
                column.settings.clear();
            }
            else
            {
                if (codec)
                    column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, data_type ? data_type : column.type, false, true, true);

                if (comment)
                    column.comment = *comment;

                if (ttl)
                    column.ttl = ttl;

                if (data_type)
                    column.type = data_type;

                if (!settings_changes.empty())
                {
                    MergeTreeColumnSettings::validate(settings_changes);
                    if (append_column_setting)
                        for (const auto & change : settings_changes)
                            column.settings.setSetting(change.name, change.value);
                    else
                        column.settings = settings_changes;
                }

                if (!settings_resets.empty())
                {
                    for (const auto & setting : settings_resets)
                        column.settings.removeSetting(setting);
                }

                /// User specified default expression or changed
                /// datatype. We have to replace default.
                if (default_expression || data_type)
                {
                    column.default_desc.kind = default_kind;
                    column.default_desc.expression = default_expression;
                }
            }
        });

    }
    else if (type == MODIFY_ORDER_BY)
    {
        auto & sorting_key = metadata.sorting_key;
        auto & primary_key = metadata.primary_key;
        if (primary_key.definition_ast == nullptr && sorting_key.definition_ast != nullptr)
        {
            /// Primary and sorting key become independent after this ALTER so
            /// we have to save the old ORDER BY expression as the new primary
            /// key.
            primary_key = KeyDescription::getKeyFromAST(sorting_key.definition_ast, metadata.columns, context);
        }

        /// Recalculate key with new order_by expression.
        sorting_key.recalculateWithNewAST(order_by, metadata.columns, context);
    }
    else if (type == MODIFY_SAMPLE_BY)
    {
        metadata.sampling_key.recalculateWithNewAST(sample_by, metadata.columns, context);
    }
    else if (type == REMOVE_SAMPLE_BY)
    {
        metadata.sampling_key = {};
    }
    else if (type == COMMENT_COLUMN)
    {
        metadata.columns.modify(column_name,
            [&](ColumnDescription & column) { column.comment = *comment; });
    }
    else if (type == COMMENT_TABLE)
    {
        metadata.comment = *comment;
    }
    else if (type == ADD_INDEX)
    {
        if (std::any_of(
                metadata.secondary_indices.cbegin(),
                metadata.secondary_indices.cend(),
                [this](const auto & index)
                {
                    return index.name == index_name;
                }))
        {
            if (if_not_exists)
                return;
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot add index {}: index with this name already exists", index_name);
        }

        auto insert_it = metadata.secondary_indices.end();

        /// insert the index in the beginning of the indices list
        if (first)
            insert_it = metadata.secondary_indices.begin();

        if (!after_index_name.empty())
        {
            insert_it = std::find_if(
                    metadata.secondary_indices.begin(),
                    metadata.secondary_indices.end(),
                    [this](const auto & index)
                    {
                        return index.name == after_index_name;
                    });

            if (insert_it == metadata.secondary_indices.end())
            {
                auto hints = metadata.secondary_indices.getHints(after_index_name);
                auto hints_string = !hints.empty() ? ", may be you meant: " + toString(hints) : "";
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong index name. Cannot find index {} to insert after{}",
                    backQuote(after_index_name), hints_string);
            }

            ++insert_it;
        }

        metadata.secondary_indices.emplace(insert_it, IndexDescription::getIndexFromAST(index_decl, metadata.columns, context));
    }
    else if (type == DROP_INDEX)
    {
        if (!partition && !clear)
        {
            auto erase_it = std::find_if(
                    metadata.secondary_indices.begin(),
                    metadata.secondary_indices.end(),
                    [this](const auto & index)
                    {
                        return index.name == index_name;
                    });

            if (erase_it == metadata.secondary_indices.end())
            {
                if (if_exists)
                    return;
                auto hints = metadata.secondary_indices.getHints(index_name);
                auto hints_string = !hints.empty() ? ", may be you meant: " + toString(hints) : "";
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong index name. Cannot find index {} to drop{}",
                    backQuote(index_name), hints_string);
            }

            metadata.secondary_indices.erase(erase_it);
        }
    }
    else if (type == ADD_STATISTICS)
    {
        for (const auto & statistics_column_name : statistics_columns)
        {
            if (!metadata.columns.has(statistics_column_name))
            {
                throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Cannot add statistics for column {}: this column is not found", statistics_column_name);
            }
        }

        auto stats_vec = ColumnStatisticsDescription::fromAST(statistics_decl, metadata.columns);
        for (const auto & [stats_column_name, stats] : stats_vec)
        {
            metadata.columns.modify(stats_column_name,
                [&](ColumnDescription & column) { column.statistics.merge(stats, column.name, column.type, if_not_exists); });
        }
    }
    else if (type == DROP_STATISTICS)
    {
        for (const auto & statistics_column_name : statistics_columns)
        {
            if (!metadata.columns.has(statistics_column_name)
                || metadata.columns.get(statistics_column_name).statistics.empty())
            {
                if (if_exists)
                    return;
                throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Wrong statistics name. Cannot find statistics {} to drop", backQuote(statistics_column_name));
            }

            if (!clear && !partition)
                metadata.columns.modify(statistics_column_name,
                    [&](ColumnDescription & column) { column.statistics.clear(); });
        }
    }
    else if (type == MODIFY_STATISTICS)
    {
        for (const auto & statistics_column_name : statistics_columns)
        {
            if (!metadata.columns.has(statistics_column_name))
            {
                throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Cannot modify statistics for column {}: this column is not found", statistics_column_name);
            }
        }

        auto stats_vec = ColumnStatisticsDescription::fromAST(statistics_decl, metadata.columns);
        for (const auto & [stats_column_name, stats] : stats_vec)
        {
            metadata.columns.modify(stats_column_name,
                [&](ColumnDescription & column) { column.statistics.assign(stats); });
        }
    }
    else if (type == ADD_CONSTRAINT)
    {
        auto constraints = metadata.constraints.getConstraints();
        if (std::any_of(
                constraints.cbegin(),
                constraints.cend(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                }))
        {
            if (if_not_exists)
                return;
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot add constraint {}: constraint with this name already exists",
                        constraint_name);
        }

        auto * insert_it = constraints.end();
        constraints.emplace(insert_it, constraint_decl);
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else if (type == DROP_CONSTRAINT)
    {
        auto constraints = metadata.constraints.getConstraints();
        auto * erase_it = std::find_if(
            constraints.begin(),
            constraints.end(),
            [this](const ASTPtr & constraint_ast) { return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name; });

        if (erase_it == constraints.end())
        {
            if (if_exists)
                return;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong constraint name. Cannot find constraint `{}` to drop",
                    constraint_name);
        }
        constraints.erase(erase_it);
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else if (type == ADD_PROJECTION)
    {
        auto projection = ProjectionDescription::getProjectionFromAST(projection_decl, metadata.columns, context);
        metadata.projections.add(std::move(projection), after_projection_name, first, if_not_exists);
    }
    else if (type == DROP_PROJECTION)
    {
        if (!partition && !clear)
            metadata.projections.remove(projection_name, if_exists);
    }
    else if (type == MODIFY_TTL)
    {
        metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            ttl, metadata.columns, context, metadata.primary_key, context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions]);
    }
    else if (type == REMOVE_TTL)
    {
        metadata.table_ttl = TTLTableDescription{};
    }
    else if (type == MODIFY_QUERY)
    {
        metadata.select = SelectQueryDescription::getSelectQueryFromASTForMatView(select, metadata.refresh != nullptr, context);
        Block as_select_sample;

        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            as_select_sample = InterpreterSelectQueryAnalyzer::getSampleBlock(select->clone(), context);
        }
        else
        {
            as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(select->clone(),
                context,
                false /* is_subquery */,
                false);
        }

        metadata.columns = ColumnsDescription(as_select_sample.getNamesAndTypesList());
    }
    else if (type == MODIFY_REFRESH)
    {
        metadata.refresh = refresh->clone();
    }
    else if (type == MODIFY_SETTING)
    {
        auto & settings_from_storage = metadata.settings_changes->as<ASTSetQuery &>().changes;
        for (const auto & change : settings_changes)
        {
            auto finder = [&change](const SettingChange & c) { return c.name == change.name; };
            auto it = std::find_if(settings_from_storage.begin(), settings_from_storage.end(), finder);

            if (it != settings_from_storage.end())
                it->value = change.value;
            else
                settings_from_storage.push_back(change);
        }
    }
    else if (type == RESET_SETTING)
    {
        auto & settings_from_storage = metadata.settings_changes->as<ASTSetQuery &>().changes;
        for (const auto & setting_name : settings_resets)
        {
            auto finder = [&setting_name](const SettingChange & c) { return c.name == setting_name; };
            auto it = std::find_if(settings_from_storage.begin(), settings_from_storage.end(), finder);

            if (it != settings_from_storage.end())
                settings_from_storage.erase(it);

            /// Intentionally ignore if there is no such setting name
        }
    }
    else if (type == RENAME_COLUMN)
    {
        metadata.columns.rename(column_name, rename_to);
        RenameColumnData rename_data{column_name, rename_to};
        RenameColumnVisitor rename_visitor(rename_data);
        for (const auto & column : metadata.columns)
        {
            metadata.columns.modify(column.name, [&](ColumnDescription & column_to_modify)
            {
                if (column_to_modify.default_desc.expression)
                    rename_visitor.visit(column_to_modify.default_desc.expression);
                if (column_to_modify.ttl)
                    rename_visitor.visit(column_to_modify.ttl);
            });
        }
        if (metadata.table_ttl.definition_ast)
            rename_visitor.visit(metadata.table_ttl.definition_ast);

        auto constraints_data = metadata.constraints.getConstraints();
        for (auto & constraint : constraints_data)
            rename_visitor.visit(constraint);
        metadata.constraints = ConstraintsDescription(constraints_data);

        if (metadata.isSortingKeyDefined())
            rename_visitor.visit(metadata.sorting_key.definition_ast);

        if (metadata.isPrimaryKeyDefined())
            rename_visitor.visit(metadata.primary_key.definition_ast);

        if (metadata.isSamplingKeyDefined())
            rename_visitor.visit(metadata.sampling_key.definition_ast);

        if (metadata.isPartitionKeyDefined())
            rename_visitor.visit(metadata.partition_key.definition_ast);

        for (auto & index : metadata.secondary_indices)
            rename_visitor.visit(index.definition_ast);
    }
    else if (type == MODIFY_SQL_SECURITY)
        metadata.setSQLSecurity(sql_security->as<ASTSQLSecurity &>());
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong parameter type in ALTER query");
}

namespace
{

/// If true, then in order to ALTER the type of the column from the type from to the type to
/// we don't need to rewrite the data, we only need to update metadata and columns.txt in part directories.
/// The function works for Arrays and Nullables of the same structure.
bool isMetadataOnlyConversion(const IDataType * from, const IDataType * to)
{
    auto is_compatible_enum_types_conversion = [](const IDataType * from_type, const IDataType * to_type)
    {
        if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from_type))
        {
            if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to_type))
                return to_enum8->contains(*from_enum8);
        }

        if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from_type))
        {
            if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to_type))
                return to_enum16->contains(*from_enum16);
        }

        return false;
    };

    static const std::unordered_multimap<std::type_index, const std::type_info &> allowed_conversions =
        {
            { typeid(DataTypeEnum8),    typeid(DataTypeInt8)     },
            { typeid(DataTypeEnum16),   typeid(DataTypeInt16)    },
            { typeid(DataTypeDateTime), typeid(DataTypeUInt32)   },
            { typeid(DataTypeUInt32),   typeid(DataTypeDateTime) },
            { typeid(DataTypeDate),     typeid(DataTypeUInt16)   },
            { typeid(DataTypeUInt16),   typeid(DataTypeDate)     },
        };

    /// Unwrap some nested and check for valid conversions
    while (true)
    {
        /// types are equal, obviously pure metadata alter
        if (from->equals(*to))
            return true;

        /// We just adding something to enum, nothing changed on disk
        if (is_compatible_enum_types_conversion(from, to))
            return true;

        /// Types changed, but representation on disk didn't
        auto it_range = allowed_conversions.equal_range(typeid(*from));
        for (auto it = it_range.first; it != it_range.second; ++it)
        {
            if (it->second == typeid(*to))
                return true;
        }

        const auto * arr_from = typeid_cast<const DataTypeArray *>(from);
        const auto * arr_to = typeid_cast<const DataTypeArray *>(to);
        if (arr_from && arr_to)
        {
            from = arr_from->getNestedType().get();
            to = arr_to->getNestedType().get();
            continue;
        }

        const auto * nullable_from = typeid_cast<const DataTypeNullable *>(from);
        const auto * nullable_to = typeid_cast<const DataTypeNullable *>(to);
        if (nullable_from && nullable_to)
        {
            from = nullable_from->getNestedType().get();
            to = nullable_to->getNestedType().get();
            continue;
        }

        return false;
    }
}

}

bool AlterCommand::isSettingsAlter() const
{
    return type == MODIFY_SETTING || type == RESET_SETTING;
}

bool AlterCommand::isRequireMutationStage(const StorageInMemoryMetadata & metadata) const
{
    if (ignore)
        return false;

    /// We remove properties on metadata level
    if (isRemovingProperty() || type == REMOVE_TTL || type == REMOVE_SAMPLE_BY)
        return false;

    if (type == DROP_INDEX || type == DROP_PROJECTION || type == RENAME_COLUMN || type == DROP_STATISTICS)
        return true;

    /// Drop alias is metadata alter, in other case mutation is required.
    if (type == DROP_COLUMN)
        return metadata.columns.hasColumnOrNested(GetColumnsOptions::AllPhysical, column_name);

    if (type != MODIFY_COLUMN || data_type == nullptr)
        return false;

    for (const auto & column : metadata.columns.getAllPhysical())
    {
        if (column.name == column_name && !isMetadataOnlyConversion(column.type.get(), data_type.get()))
            return true;
    }
    return false;
}

bool AlterCommand::isCommentAlter() const
{
    if (type == COMMENT_COLUMN || type == COMMENT_TABLE)
    {
        return true;
    }
    if (type == MODIFY_COLUMN)
    {
        return comment.has_value() && codec == nullptr && data_type == nullptr && default_expression == nullptr && ttl == nullptr;
    }
    return false;
}

bool AlterCommand::isTTLAlter(const StorageInMemoryMetadata & metadata) const
{
    if (type == MODIFY_TTL)
    {
        if (!metadata.table_ttl.definition_ast)
            return true;
        /// If TTL had not been changed, do not require mutations
        return queryToString(metadata.table_ttl.definition_ast) != queryToString(ttl);
    }

    if (!ttl || type != MODIFY_COLUMN)
        return false;

    bool column_ttl_changed = true;
    for (const auto & [name, ttl_ast] : metadata.columns.getColumnTTLs())
    {
        if (name == column_name && queryToString(*ttl) == queryToString(*ttl_ast))
        {
            column_ttl_changed = false;
            break;
        }
    }

    return column_ttl_changed;
}

bool AlterCommand::isRemovingProperty() const
{
    return to_remove != RemoveProperty::NO_PROPERTY;
}

bool AlterCommand::isDropSomething() const
{
    return type == Type::DROP_COLUMN || type == Type::DROP_INDEX || type == Type::DROP_STATISTICS
        || type == Type::DROP_CONSTRAINT || type == Type::DROP_PROJECTION;
}

std::optional<MutationCommand> AlterCommand::tryConvertToMutationCommand(StorageInMemoryMetadata & metadata, ContextPtr context) const
{
    if (!isRequireMutationStage(metadata))
        return {};

    MutationCommand result;

    if (type == MODIFY_COLUMN)
    {
        result.type = MutationCommand::Type::READ_COLUMN;
        result.column_name = column_name;
        result.data_type = data_type;
        result.predicate = nullptr;
    }
    else if (type == DROP_COLUMN)
    {
        result.type = MutationCommand::Type::DROP_COLUMN;
        result.column_name = column_name;
        if (clear)
            result.clear = true;
        if (partition)
            result.partition = partition;
        result.predicate = nullptr;
    }
    else if (type == DROP_INDEX)
    {
        result.type = MutationCommand::Type::DROP_INDEX;
        result.column_name = index_name;
        if (clear)
            result.clear = true;
        if (partition)
            result.partition = partition;

        result.predicate = nullptr;
    }
    else if (type == DROP_STATISTICS)
    {
        result.type = MutationCommand::Type::DROP_STATISTICS;
        result.statistics_columns = statistics_columns;

        if (clear)
            result.clear = true;
        if (partition)
            result.partition = partition;

        result.predicate = nullptr;
    }
    else if (type == DROP_PROJECTION)
    {
        result.type = MutationCommand::Type::DROP_PROJECTION;
        result.column_name = projection_name;
        if (clear)
            result.clear = true;
        if (partition)
            result.partition = partition;

        result.predicate = nullptr;
    }
    else if (type == RENAME_COLUMN)
    {
        result.type = MutationCommand::Type::RENAME_COLUMN;
        result.column_name = column_name;
        result.rename_to = rename_to;
    }

    result.ast = ast->clone();
    apply(metadata, context);
    return result;
}

bool AlterCommands::hasFullTextIndex(const StorageInMemoryMetadata & metadata)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == FULL_TEXT_INDEX_NAME)
            return true;
    }
    return false;
}

bool AlterCommands::hasLegacyInvertedIndex(const StorageInMemoryMetadata & metadata)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == INVERTED_INDEX_NAME)
            return true;
    }
    return false;
}

bool AlterCommands::hasVectorSimilarityIndex(const StorageInMemoryMetadata & metadata)
{
    for (const auto & index : metadata.secondary_indices)
    {
        if (index.type == "vector_similarity")
            return true;
    }
    return false;
}

void AlterCommands::apply(StorageInMemoryMetadata & metadata, ContextPtr context) const
{
    if (!prepared)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Alter commands is not prepared. Cannot apply. It's a bug");

    auto metadata_copy = metadata;

    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(metadata_copy, context);

    /// Changes in columns may lead to changes in keys expression.
    metadata_copy.sorting_key.recalculateWithNewAST(metadata_copy.sorting_key.definition_ast, metadata_copy.columns, context);
    if (metadata_copy.primary_key.definition_ast != nullptr)
    {
        metadata_copy.primary_key.recalculateWithNewAST(metadata_copy.primary_key.definition_ast, metadata_copy.columns, context);
    }
    else
    {
        metadata_copy.primary_key = KeyDescription::getKeyFromAST(metadata_copy.sorting_key.definition_ast, metadata_copy.columns, context);
        metadata_copy.primary_key.definition_ast = nullptr;
    }

    /// And in partition key expression
    if (metadata_copy.partition_key.definition_ast != nullptr)
    {
        metadata_copy.partition_key.recalculateWithNewAST(metadata_copy.partition_key.definition_ast, metadata_copy.columns, context);

        /// If partition key expression is changed, we also need to rebuild minmax_count_projection
        if (!blocksHaveEqualStructure(metadata_copy.partition_key.sample_block, metadata.partition_key.sample_block))
        {
            auto minmax_columns = metadata_copy.getColumnsRequiredForPartitionKey();
            auto partition_key = metadata_copy.partition_key.expression_list_ast->clone();
            FunctionNameNormalizer::visit(partition_key.get());
            auto primary_key_asts = metadata_copy.primary_key.expression_list_ast->children;
            metadata_copy.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
                metadata_copy.columns, partition_key, minmax_columns, primary_key_asts, context));
        }
    }

    // /// And in sample key expression
    if (metadata_copy.sampling_key.definition_ast != nullptr)
        metadata_copy.sampling_key.recalculateWithNewAST(metadata_copy.sampling_key.definition_ast, metadata_copy.columns, context);

    /// Changes in columns may lead to changes in secondary indices
    for (auto & index : metadata_copy.secondary_indices)
    {
        try
        {
            index = IndexDescription::getIndexFromAST(index.definition_ast, metadata_copy.columns, context);
        }
        catch (Exception & exception)
        {
            exception.addMessage("Cannot apply mutation because it breaks skip index " + index.name);
            throw;
        }
    }

    /// Changes in columns may lead to changes in projections
    ProjectionsDescription new_projections;
    for (const auto & projection : metadata_copy.projections)
    {
        try
        {
            /// Check if we can still build projection from new metadata.
            auto new_projection = ProjectionDescription::getProjectionFromAST(projection.definition_ast, metadata_copy.columns, context);
            /// Check if new metadata has the same keys as the old one.
            if (!blocksHaveEqualStructure(projection.sample_block_for_keys, new_projection.sample_block_for_keys))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot ALTER column");
            /// Check if new metadata is convertible from old metadata for projection.
            Block old_projection_block = projection.sample_block;
            performRequiredConversions(old_projection_block, new_projection.sample_block.getNamesAndTypesList(), context);
            new_projections.add(std::move(new_projection));
        }
        catch (Exception & exception)
        {
            exception.addMessage("Cannot apply mutation because it breaks projection " + projection.name);
            throw;
        }
    }
    metadata_copy.projections = std::move(new_projections);

    /// Changes in columns may lead to changes in TTL expressions.
    auto column_ttl_asts = metadata_copy.columns.getColumnTTLs();
    metadata_copy.column_ttls_by_name.clear();
    for (const auto & [name, ast] : column_ttl_asts)
    {
        auto new_ttl_entry = TTLDescription::getTTLFromAST(
            ast, metadata_copy.columns, context, metadata_copy.primary_key, context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions]);
        metadata_copy.column_ttls_by_name[name] = new_ttl_entry;
    }

    if (metadata_copy.table_ttl.definition_ast != nullptr)
        metadata_copy.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            metadata_copy.table_ttl.definition_ast,
            metadata_copy.columns,
            context,
            metadata_copy.primary_key,
            context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions]);

    metadata = std::move(metadata_copy);
}


void AlterCommands::prepare(const StorageInMemoryMetadata & metadata)
{
    auto columns = metadata.columns;

    auto ast_to_str = [](const ASTPtr & query) -> String
    {
        if (!query)
            return "";
        return queryToString(query);
    };

    for (size_t i = 0; i < size(); ++i)
    {
        auto & command = (*this)[i];
        bool has_column = columns.has(command.column_name) || columns.hasNested(command.column_name);
        if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;

            if (has_column)
            {
                const auto & column_from_table = columns.get(command.column_name);
                if (command.data_type && !command.default_expression && column_from_table.default_desc.expression)
                {
                    command.default_kind = column_from_table.default_desc.kind;
                    command.default_expression = column_from_table.default_desc.expression;
                }

            }
        }
        else if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (has_column && command.if_not_exists)
                command.ignore = true;
        }
        else if (command.type == AlterCommand::DROP_COLUMN
                || command.type == AlterCommand::COMMENT_COLUMN
                || command.type == AlterCommand::RENAME_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;
        }
        else if (command.type == AlterCommand::MODIFY_ORDER_BY)
        {
            if (ast_to_str(command.order_by) == ast_to_str(metadata.sorting_key.definition_ast))
                command.ignore = true;
        }
    }

    prepared = true;
}


void AlterCommands::validate(const StoragePtr & table, ContextPtr context) const
{
    const auto & metadata = table->getInMemoryMetadata();
    auto virtuals = table->getVirtualsPtr();

    auto all_columns = metadata.columns;
    /// Default expression for all added/modified columns
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NameSet modified_columns, renamed_columns;
    for (size_t i = 0; i < size(); ++i)
    {
        const auto & command = (*this)[i];

        if (command.ttl && !table->supportsTTL())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Engine {} doesn't support TTL clause", table->getName());

        const auto & column_name = command.column_name;
        if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (all_columns.has(column_name) || all_columns.hasNested(column_name))
            {
                if (!command.if_not_exists)
                    throw Exception(ErrorCodes::DUPLICATE_COLUMN,
                                    "Cannot add column {}: column with this name already exists",
                                    backQuote(column_name));
                continue;
            }

            if (!command.data_type)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Data type have to be specified for column {} to add", backQuote(column_name));

            validateDataType(command.data_type, DataTypeValidationSettings(context->getSettingsRef()));
            checkAllTypesAreAllowedInTable(NamesAndTypesList{{command.column_name, command.data_type}});

            /// FIXME: Adding a new column of type Object(JSON) is broken.
            /// Looks like there is something around default expression for this column (method `getDefault` is not implemented for the data type Object).
            /// But after ALTER TABLE ADD COLUMN we need to fill existing rows with something (exactly the default value).
            /// So we don't allow to do it for now.
            if (command.data_type->hasDynamicSubcolumnsDeprecated())
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Adding a new column of a type which has dynamic subcolumns to an existing table is not allowed. It has known bugs");

            if (virtuals->tryGet(column_name, VirtualsKind::Persistent))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot add column {}: this column name is reserved for persistent virtual column", backQuote(column_name));

            if (command.codec)
            {
                const auto & settings = context->getSettingsRef();
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    command.codec,
                    command.data_type,
                    !settings[Setting::allow_suspicious_codecs],
                    settings[Setting::allow_experimental_codecs],
                    settings[Setting::enable_zstd_qat_codec]);
            }

            all_columns.add(ColumnDescription(column_name, command.data_type));
        }
        else if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!all_columns.has(column_name))
            {
                if (!command.if_exists)
                {
                    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Wrong column. Cannot find column {} to modify{}",
                                    backQuote(column_name), all_columns.getHintsMessage(column_name));
                }
                continue;
            }

            if (renamed_columns.contains(column_name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename and modify the same column {} "
                                                             "in a single ALTER query", backQuote(column_name));

            if (command.codec)
            {
                if (all_columns.hasAlias(column_name))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot specify codec for column type ALIAS");
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    command.codec,
                    command.data_type,
                    !context->getSettingsRef()[Setting::allow_suspicious_codecs],
                    context->getSettingsRef()[Setting::allow_experimental_codecs],
                    context->getSettingsRef()[Setting::enable_zstd_qat_codec]);
            }
            auto column_default = all_columns.getDefault(column_name);
            if (column_default)
            {
                if (command.to_remove == AlterCommand::RemoveProperty::DEFAULT && column_default->kind != ColumnDefaultKind::Default)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot remove DEFAULT from column {}, because column default type is {}. Use REMOVE {} to delete it",
                            backQuote(column_name), toString(column_default->kind), toString(column_default->kind));
                }
                if (command.to_remove == AlterCommand::RemoveProperty::MATERIALIZED && column_default->kind != ColumnDefaultKind::Materialized)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot remove MATERIALIZED from column {}, because column default type is {}. Use REMOVE {} to delete it",
                        backQuote(column_name), toString(column_default->kind), toString(column_default->kind));
                }
                if (command.to_remove == AlterCommand::RemoveProperty::ALIAS && column_default->kind != ColumnDefaultKind::Alias)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Cannot remove ALIAS from column {}, because column default type is {}. Use REMOVE {} to delete it",
                        backQuote(column_name), toString(column_default->kind), toString(column_default->kind));
                }
            }

            /// FIXME: Modifying the column to/from Object(JSON) is broken.
            /// Looks like there is something around default expression for this column (method `getDefault` is not implemented for the data type Object).
            /// But after ALTER TABLE MODIFY COLUMN we need to fill existing rows with something (exactly the default value) or calculate the common type for it.
            /// So we don't allow to do it for now.
            if (command.data_type)
            {
                validateDataType(command.data_type, DataTypeValidationSettings(context->getSettingsRef()));
                checkAllTypesAreAllowedInTable(NamesAndTypesList{{command.column_name, command.data_type}});

                const GetColumnsOptions options(GetColumnsOptions::All);
                const auto old_data_type = all_columns.getColumn(options, column_name).type;

                bool new_type_has_deprecated_object = command.data_type->hasDynamicSubcolumnsDeprecated();
                bool old_type_has_deprecated_object = old_data_type->hasDynamicSubcolumnsDeprecated();

                if (new_type_has_deprecated_object || old_type_has_deprecated_object)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The change of data type {} of column {} to {} is not allowed. It has known bugs",
                        old_data_type->getName(), backQuote(column_name), command.data_type->getName());

                bool has_object_type = isObject(command.data_type);
                command.data_type->forEachChild([&](const IDataType & type){ has_object_type |= isObject(type); });
                if (has_object_type)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The change of data type {} of column {} to {} is not supported.",
                        old_data_type->getName(), backQuote(column_name), command.data_type->getName());
            }

            if (command.isRemovingProperty())
            {
                if (!column_default && command.to_remove == AlterCommand::RemoveProperty::DEFAULT)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have DEFAULT, cannot remove it",
                        backQuote(column_name));

                if (!column_default && command.to_remove == AlterCommand::RemoveProperty::ALIAS)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have ALIAS, cannot remove it",
                        backQuote(column_name));

                if (!column_default && command.to_remove == AlterCommand::RemoveProperty::MATERIALIZED)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have MATERIALIZED, cannot remove it",
                        backQuote(column_name));

                const auto & column_from_table = all_columns.get(column_name);
                if (command.to_remove == AlterCommand::RemoveProperty::TTL && column_from_table.ttl == nullptr)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have TTL, cannot remove it",
                        backQuote(column_name));
                if (command.to_remove == AlterCommand::RemoveProperty::CODEC && column_from_table.codec == nullptr)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have TTL, cannot remove it",
                        backQuote(column_name));
                if (command.to_remove == AlterCommand::RemoveProperty::COMMENT && column_from_table.comment.empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column {} doesn't have COMMENT, cannot remove it",
                        backQuote(column_name));
            }

            modified_columns.emplace(column_name);
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (all_columns.has(command.column_name) || all_columns.hasNested(command.column_name))
            {
                if (!command.clear) /// CLEAR column is Ok even if there are dependencies.
                {
                    /// Check if we are going to DROP a column that some other columns depend on.
                    for (const ColumnDescription & column : all_columns)
                    {
                        const auto & default_expression = column.default_desc.expression;
                        if (default_expression)
                        {
                            ASTPtr query = default_expression->clone();
                            auto syntax_result = TreeRewriter(context).analyze(query, all_columns.getAll());
                            const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
                            const auto required_columns = actions->getRequiredColumns();

                            if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot drop column {}, because column {} depends on it",
                                        backQuote(command.column_name), backQuote(column.name));
                        }
                    }
                }
                all_columns.remove(command.column_name);
            }
            else if (!command.if_exists)
            {
                 auto message = PreformattedMessage::create(
                    "Wrong column name. Cannot find column {} to drop", backQuote(command.column_name));
                all_columns.appendHintsMessage(message.text, command.column_name);
                throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
            }
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                {
                    auto message = PreformattedMessage::create(
                        "Wrong column name. Cannot find column {} to comment", backQuote(command.column_name));
                    all_columns.appendHintsMessage(message.text, command.column_name);
                    throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                }
            }
        }
        else if (command.type == AlterCommand::MODIFY_SETTING || command.type == AlterCommand::RESET_SETTING)
        {
            if (metadata.settings_changes == nullptr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot alter settings, because table engine doesn't support settings changes");
        }
        else if (command.type == AlterCommand::RENAME_COLUMN)
        {
           for (size_t j = i + 1; j < size(); ++j)
           {
               auto next_command = (*this)[j];
               if (next_command.type == AlterCommand::RENAME_COLUMN)
               {
                   if (next_command.column_name == command.rename_to)
                       throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transitive renames in a single ALTER query are not allowed (don't make sense)");
                   if (next_command.column_name == command.column_name)
                       throw Exception(
                           ErrorCodes::BAD_ARGUMENTS,
                           "Cannot rename column '{}' to two different names in a single ALTER query",
                           backQuote(command.column_name));
               }
           }

            /// TODO Implement nested rename
            if (all_columns.hasNested(command.column_name))
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename whole Nested struct");
            }

            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                {
                    auto message = PreformattedMessage::create(
                       "Wrong column name. Cannot find column {} to rename", backQuote(command.column_name));
                    all_columns.appendHintsMessage(message.text, command.column_name);
                    throw Exception(std::move(message), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                }
                continue;
            }

            if (all_columns.has(command.rename_to))
                throw Exception(ErrorCodes::DUPLICATE_COLUMN,
                    "Cannot rename to {}: column with this name already exists", backQuote(command.rename_to));

            if (virtuals->tryGet(command.rename_to, VirtualsKind::Persistent))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot rename to {}: this column name is reserved for persistent virtual column", backQuote(command.rename_to));

            if (modified_columns.contains(column_name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot rename and modify the same column {} "
                                                             "in a single ALTER query", backQuote(column_name));

            String from_nested_table_name = Nested::extractTableName(command.column_name);
            String to_nested_table_name = Nested::extractTableName(command.rename_to);
            bool from_nested = from_nested_table_name != command.column_name;
            bool to_nested = to_nested_table_name != command.rename_to;

            if (from_nested && to_nested)
            {
                if (from_nested_table_name != to_nested_table_name)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot rename column from one nested name to another");
            }
            else if (!from_nested && !to_nested)
            {
                all_columns.rename(command.column_name, command.rename_to);
                renamed_columns.emplace(command.column_name);
                renamed_columns.emplace(command.rename_to);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot rename column from nested struct to normal column and vice versa");
            }
        }
        else if (command.type == AlterCommand::REMOVE_TTL && !metadata.hasAnyTableTTL())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table doesn't have any table TTL expression, cannot remove");
        }
        else if (command.type == AlterCommand::REMOVE_SAMPLE_BY && !metadata.hasSamplingKey())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table doesn't have SAMPLE BY, cannot remove");
        }

        /// Collect default expressions for MODIFY and ADD commands
        if (command.type == AlterCommand::MODIFY_COLUMN || command.type == AlterCommand::ADD_COLUMN)
        {
            if (command.default_expression)
            {
                DataTypePtr data_type_ptr;
                /// If we modify default, but not type.
                if (!command.data_type) /// it's not ADD COLUMN, because we cannot add column without type
                    data_type_ptr = all_columns.get(column_name).type;
                else
                    data_type_ptr = command.data_type;

                const auto & final_column_name = column_name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());

                default_expr_list->children.emplace_back(setAlias(
                    addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()),
                    final_column_name));

                default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), tmp_column_name));
            } /// if we change data type for column with default
            else if (all_columns.has(column_name) && command.data_type)
            {
                const auto & column_in_table = all_columns.get(column_name);
                /// Column doesn't have a default, nothing to check
                if (!column_in_table.default_desc.expression)
                    continue;

                const auto & final_column_name = column_name;
                const auto tmp_column_name = final_column_name + "_tmp_alter" + toString(randomSeed());
                const auto data_type_ptr = command.data_type;

                default_expr_list->children.emplace_back(setAlias(
                    addTypeConversionToAST(std::make_shared<ASTIdentifier>(tmp_column_name), data_type_ptr->getName()), final_column_name));

                default_expr_list->children.emplace_back(setAlias(column_in_table.default_desc.expression->clone(), tmp_column_name));
            }
        }
    }

    /// Parameterized views do not have 'columns' in their metadata
    bool is_parameterized_view = table->as<StorageView>() && table->as<StorageView>()->isParameterizedView();

    if (!is_parameterized_view && all_columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot DROP or CLEAR all columns");

    validateColumnsDefaultsAndGetSampleBlock(default_expr_list, all_columns.getAll(), context);
}

bool AlterCommands::hasNonReplicatedAlterCommand() const
{
    return std::any_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter() || c.isCommentAlter(); });
}

bool AlterCommands::areNonReplicatedAlterCommands() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter() || c.isCommentAlter(); });
}

bool AlterCommands::isSettingsAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });
}

bool AlterCommands::isCommentAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isCommentAlter(); });
}

static MutationCommand createMaterializeTTLCommand()
{
    MutationCommand command;
    auto ast = std::make_shared<ASTAlterCommand>();
    ast->type = ASTAlterCommand::MATERIALIZE_TTL;
    command.type = MutationCommand::MATERIALIZE_TTL;
    command.ast = std::move(ast);
    return command;
}

MutationCommands AlterCommands::getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, ContextPtr context, bool with_alters) const
{
    MutationCommands result;
    for (const auto & alter_cmd : *this)
    {
        if (auto mutation_cmd = alter_cmd.tryConvertToMutationCommand(metadata, context); mutation_cmd)
        {
            result.push_back(*mutation_cmd);
        }
        else if (with_alters)
        {
            result.push_back(MutationCommand{.ast = alter_cmd.ast->clone(), .type = MutationCommand::Type::ALTER_WITHOUT_MUTATION});
        }
    }

    if (materialize_ttl)
    {
        for (const auto & alter_cmd : *this)
        {
            if (alter_cmd.isTTLAlter(metadata))
            {
                result.push_back(createMaterializeTTLCommand());
                break;
            }
        }
    }

    return result;
}

}
