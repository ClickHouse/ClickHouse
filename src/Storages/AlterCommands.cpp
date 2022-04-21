#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>
#include <Common/randomSeed.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

AlterCommand::RemoveProperty removePropertyFromString(const String & property)
{
    if (property.empty())
        return AlterCommand::RemoveProperty::NO_PROPERTY;
    else if (property == "DEFAULT")
        return AlterCommand::RemoveProperty::DEFAULT;
    else if (property == "MATERIALIZED")
        return AlterCommand::RemoveProperty::MATERIALIZED;
    else if (property == "ALIAS")
        return AlterCommand::RemoveProperty::ALIAS;
    else if (property == "COMMENT")
        return AlterCommand::RemoveProperty::COMMENT;
    else if (property == "CODEC")
        return AlterCommand::RemoveProperty::CODEC;
    else if (property == "TTL")
        return AlterCommand::RemoveProperty::TTL;

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
            command.comment = ast_comment.value.get<String>();
        }

        if (ast_col_decl.codec)
        {
            if (ast_col_decl.default_specifier == "ALIAS")
                throw Exception{"Cannot specify codec for column type ALIAS", ErrorCodes::BAD_ARGUMENTS};
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
    else if (command_ast->type == ASTAlterCommand::DROP_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_column)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_COLUMN)
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
            command.comment.emplace(ast_comment.value.get<String>());
        }

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        if (ast_col_decl.codec)
            command.codec = ast_col_decl.codec;

        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        command.first = command_ast->first;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::COMMENT_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.get<String>();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_COMMENT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = COMMENT_TABLE;
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.get<String>();
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_ORDER_BY;
        command.order_by = command_ast->order_by;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SAMPLE_BY;
        command.sample_by = command_ast->sample_by;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::REMOVE_SAMPLE_BY;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.index_decl = command_ast->index_decl;
        command.type = AlterCommand::ADD_INDEX;

        const auto & ast_index_decl = command_ast->index_decl->as<ASTIndexDeclaration &>();

        command.index_name = ast_index_decl.name;

        if (command_ast->index)
            command.after_index_name = command_ast->index->as<ASTIdentifier &>().name();

        command.if_not_exists = command_ast->if_not_exists;
        command.first = command_ast->first;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.constraint_decl = command_ast->constraint_decl;
        command.type = AlterCommand::ADD_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.projection_decl = command_ast->projection_decl;
        command.type = AlterCommand::ADD_PROJECTION;

        const auto & ast_projection_decl = command_ast->projection_decl->as<ASTProjectionDeclaration &>();

        command.projection_name = ast_projection_decl.name;

        if (command_ast->projection)
            command.after_projection_name = command_ast->projection->as<ASTIdentifier &>().name();

        command.first = command_ast->first;
        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.if_exists = command_ast->if_exists;
        command.type = AlterCommand::DROP_CONSTRAINT;
        command.constraint_name = command_ast->constraint->as<ASTIdentifier &>().name();

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_INDEX)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = command_ast->index->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_index)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_PROJECTION)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::DROP_PROJECTION;
        command.projection_name = command_ast->projection->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        if (command_ast->clear_projection)
            command.clear = true;

        if (command_ast->partition)
            command.partition = command_ast->partition;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_TTL;
        command.ttl = command_ast->ttl;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::REMOVE_TTL)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::REMOVE_TTL;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_DATABASE_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::RESET_SETTING)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RESET_SETTING;
        for (const ASTPtr & identifier_ast : command_ast->settings_resets->children)
        {
            const auto & identifier = identifier_ast->as<ASTIdentifier &>();
            auto insertion = command.settings_resets.emplace(identifier.name());
            if (!insertion.second)
                throw Exception("Duplicate setting name " + backQuote(identifier.name()),
                                ErrorCodes::BAD_ARGUMENTS);
        }
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_QUERY)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::MODIFY_QUERY;
        command.select = command_ast->select;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::RENAME_COLUMN)
    {
        AlterCommand command;
        command.ast = command_ast->clone();
        command.type = AlterCommand::RENAME_COLUMN;
        command.column_name = command_ast->column->as<ASTIdentifier &>().name();
        command.rename_to = command_ast->rename_to->as<ASTIdentifier &>().name();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    else
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
            column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, data_type, false, true);

        column.ttl = ttl;

        metadata.columns.add(column, after_column, first);

        /// Slow, because each time a list is copied
        if (context->getSettingsRef().flatten_nested)
            metadata.columns.flattenNested();
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
            else
            {
                if (codec)
                    column.codec = CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(codec, data_type ? data_type : column.type, false, true);

                if (comment)
                    column.comment = *comment;

                if (ttl)
                    column.ttl = ttl;

                if (data_type)
                    column.type = data_type;

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
            else
                throw Exception{"Cannot add index " + index_name + ": index with this name already exists",
                                ErrorCodes::ILLEGAL_COLUMN};
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
                throw Exception(
                    "Wrong index name. Cannot find index " + backQuote(after_index_name) + " to insert after" + hints_string,
                    ErrorCodes::BAD_ARGUMENTS);
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
                throw Exception(
                    "Wrong index name. Cannot find index " + backQuote(index_name) + " to drop" + hints_string, ErrorCodes::BAD_ARGUMENTS);
            }

            metadata.secondary_indices.erase(erase_it);
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
            throw Exception("Cannot add constraint " + constraint_name + ": constraint with this name already exists",
                        ErrorCodes::ILLEGAL_COLUMN);
        }

        auto insert_it = constraints.end();
        constraints.emplace(insert_it, constraint_decl);
        metadata.constraints = ConstraintsDescription(constraints);
    }
    else if (type == DROP_CONSTRAINT)
    {
        auto constraints = metadata.constraints.getConstraints();
        auto erase_it = std::find_if(
                constraints.begin(),
                constraints.end(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                });

        if (erase_it == constraints.end())
        {
            if (if_exists)
                return;
            throw Exception("Wrong constraint name. Cannot find constraint `" + constraint_name + "` to drop",
                    ErrorCodes::BAD_ARGUMENTS);
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
        metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(ttl, metadata.columns, context, metadata.primary_key);
    }
    else if (type == REMOVE_TTL)
    {
        metadata.table_ttl = TTLTableDescription{};
    }
    else if (type == MODIFY_QUERY)
    {
        metadata.select = SelectQueryDescription::getSelectQueryFromASTForMatView(select, context);
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
    else
        throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
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

    /// Unwrap some nested and check for valid conevrsions
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
        if (nullable_to)
        {
            from = nullable_from ? nullable_from->getNestedType().get() : from;
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

    if (type == DROP_INDEX || type == DROP_PROJECTION || type == RENAME_COLUMN)
        return true;

    /// Drop alias is metadata alter, in other case mutation is required.
    if (type == DROP_COLUMN)
        return metadata.columns.hasPhysical(column_name);

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
    else if (type == MODIFY_COLUMN)
    {
        return comment.has_value()
            && codec == nullptr
            && data_type == nullptr
            && default_expression == nullptr
            && ttl == nullptr;
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


void AlterCommands::apply(StorageInMemoryMetadata & metadata, ContextPtr context) const
{
    if (!prepared)
        throw DB::Exception("Alter commands is not prepared. Cannot apply. It's a bug", ErrorCodes::LOGICAL_ERROR);

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
        metadata_copy.partition_key.recalculateWithNewAST(metadata_copy.partition_key.definition_ast, metadata_copy.columns, context);

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
            new_projections.add(ProjectionDescription::getProjectionFromAST(projection.definition_ast, metadata_copy.columns, context));
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
        auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, metadata_copy.columns, context, metadata_copy.primary_key);
        metadata_copy.column_ttls_by_name[name] = new_ttl_entry;
    }

    if (metadata_copy.table_ttl.definition_ast != nullptr)
        metadata_copy.table_ttl = TTLTableDescription::getTTLForTableFromAST(
            metadata_copy.table_ttl.definition_ast, metadata_copy.columns, context, metadata_copy.primary_key);

    metadata = std::move(metadata_copy);
}


void AlterCommands::prepare(const StorageInMemoryMetadata & metadata)
{
    auto columns = metadata.columns;

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
                auto column_from_table = columns.get(command.column_name);
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
    }
    prepared = true;
}


void AlterCommands::validate(const StoragePtr & table, ContextPtr context) const
{
    const StorageInMemoryMetadata & metadata = table->getInMemoryMetadata();
    auto all_columns = metadata.columns;
    /// Default expression for all added/modified columns
    ASTPtr default_expr_list = std::make_shared<ASTExpressionList>();
    NameSet modified_columns, renamed_columns;
    for (size_t i = 0; i < size(); ++i)
    {
        const auto & command = (*this)[i];

        if (command.ttl && !table->supportsTTL())
            throw Exception("Engine " + table->getName() + " doesn't support TTL clause", ErrorCodes::BAD_ARGUMENTS);

        const auto & column_name = command.column_name;
        if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (all_columns.has(column_name) || all_columns.hasNested(column_name))
            {
                if (!command.if_not_exists)
                    throw Exception{"Cannot add column " + backQuote(column_name) + ": column with this name already exists",
                                    ErrorCodes::DUPLICATE_COLUMN};
                else
                    continue;
            }

            if (!command.data_type)
                throw Exception{"Data type have to be specified for column " + backQuote(column_name) + " to add",
                                ErrorCodes::BAD_ARGUMENTS};

            if (command.codec)
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(command.codec, command.data_type, !context->getSettingsRef().allow_suspicious_codecs, context->getSettingsRef().allow_experimental_codecs);

            all_columns.add(ColumnDescription(column_name, command.data_type));
        }
        else if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!all_columns.has(column_name))
            {
                if (!command.if_exists)
                {
                    String exception_message = fmt::format("Wrong column. Cannot find column {} to modify", backQuote(column_name));
                    all_columns.appendHintsMessage(exception_message, column_name);
                    throw Exception{exception_message,
                        ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK};
                }
                else
                    continue;
            }

            if (renamed_columns.contains(column_name))
                throw Exception{"Cannot rename and modify the same column " + backQuote(column_name) + " in a single ALTER query",
                                ErrorCodes::NOT_IMPLEMENTED};

            if (command.codec)
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(command.codec, command.data_type, !context->getSettingsRef().allow_suspicious_codecs, context->getSettingsRef().allow_experimental_codecs);
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

                auto column_from_table = all_columns.get(column_name);
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
                                throw Exception("Cannot drop column " + backQuote(command.column_name)
                                        + ", because column " + backQuote(column.name) + " depends on it",
                                    ErrorCodes::ILLEGAL_COLUMN);
                        }
                    }
                }
                all_columns.remove(command.column_name);
            }
            else if (!command.if_exists)
            {
                String exception_message = fmt::format("Wrong column name. Cannot find column {} to drop", backQuote(command.column_name));
                all_columns.appendHintsMessage(exception_message, command.column_name);
                throw Exception(exception_message, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
            }
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                {
                    String exception_message = fmt::format("Wrong column name. Cannot find column {} to comment", backQuote(command.column_name));
                    all_columns.appendHintsMessage(exception_message, command.column_name);
                    throw Exception(exception_message, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                }
            }
        }
        else if (command.type == AlterCommand::MODIFY_SETTING || command.type == AlterCommand::RESET_SETTING)
        {
            if (metadata.settings_changes == nullptr)
                throw Exception{"Cannot alter settings, because table engine doesn't support settings changes", ErrorCodes::BAD_ARGUMENTS};
        }
        else if (command.type == AlterCommand::RENAME_COLUMN)
        {
           for (size_t j = i + 1; j < size(); ++j)
           {
               auto next_command = (*this)[j];
               if (next_command.type == AlterCommand::RENAME_COLUMN)
               {
                   if (next_command.column_name == command.rename_to)
                       throw Exception{"Transitive renames in a single ALTER query are not allowed (don't make sense)",
                                                            ErrorCodes::NOT_IMPLEMENTED};
                   else if (next_command.column_name == command.column_name)
                       throw Exception{"Cannot rename column '" + backQuote(command.column_name)
                                           + "' to two different names in a single ALTER query",
                                       ErrorCodes::BAD_ARGUMENTS};
               }
           }

            /// TODO Implement nested rename
            if (all_columns.hasNested(command.column_name))
            {
                throw Exception{"Cannot rename whole Nested struct", ErrorCodes::NOT_IMPLEMENTED};
            }

            if (!all_columns.has(command.column_name))
            {
                if (!command.if_exists)
                {
                    String exception_message = fmt::format("Wrong column name. Cannot find column {} to rename", backQuote(command.column_name));
                    all_columns.appendHintsMessage(exception_message, command.column_name);
                    throw Exception(exception_message, ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
                }
                else
                    continue;
            }

            if (all_columns.has(command.rename_to))
                throw Exception{"Cannot rename to " + backQuote(command.rename_to) + ": column with this name already exists",
                                ErrorCodes::DUPLICATE_COLUMN};

            if (modified_columns.contains(column_name))
                throw Exception{"Cannot rename and modify the same column " + backQuote(column_name) + " in a single ALTER query",
                                ErrorCodes::NOT_IMPLEMENTED};

            String from_nested_table_name = Nested::extractTableName(command.column_name);
            String to_nested_table_name = Nested::extractTableName(command.rename_to);
            bool from_nested = from_nested_table_name != command.column_name;
            bool to_nested = to_nested_table_name != command.rename_to;

            if (from_nested && to_nested)
            {
                if (from_nested_table_name != to_nested_table_name)
                    throw Exception{"Cannot rename column from one nested name to another", ErrorCodes::BAD_ARGUMENTS};
            }
            else if (!from_nested && !to_nested)
            {
                all_columns.rename(command.column_name, command.rename_to);
                renamed_columns.emplace(command.column_name);
                renamed_columns.emplace(command.rename_to);
            }
            else
            {
                throw Exception{"Cannot rename column from nested struct to normal column and vice versa", ErrorCodes::BAD_ARGUMENTS};
            }
        }
        else if (command.type == AlterCommand::REMOVE_TTL && !metadata.hasAnyTableTTL())
        {
            throw Exception{"Table doesn't have any table TTL expression, cannot remove", ErrorCodes::BAD_ARGUMENTS};
        }
        else if (command.type == AlterCommand::REMOVE_SAMPLE_BY && !metadata.hasSamplingKey())
        {
            throw Exception{"Table doesn't have SAMPLE BY, cannot remove", ErrorCodes::BAD_ARGUMENTS};
        }

        /// Collect default expressions for MODIFY and ADD comands
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
                auto column_in_table = all_columns.get(column_name);
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

    if (all_columns.empty())
        throw Exception{"Cannot DROP or CLEAR all columns", ErrorCodes::BAD_ARGUMENTS};

    validateColumnsDefaultsAndGetSampleBlock(default_expr_list, all_columns.getAll(), context);
}

bool AlterCommands::hasSettingsAlterCommand() const
{
    return std::any_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });
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

MutationCommands AlterCommands::getMutationCommands(StorageInMemoryMetadata metadata, bool materialize_ttl, ContextPtr context) const
{
    MutationCommands result;
    for (const auto & alter_cmd : *this)
        if (auto mutation_cmd = alter_cmd.tryConvertToMutationCommand(metadata, context); mutation_cmd)
            result.push_back(*mutation_cmd);

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
