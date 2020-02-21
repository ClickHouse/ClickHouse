#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionFactory.h>

#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_SETTING;
    extern const int DUPLICATE_COLUMN;
}


std::optional<AlterCommand> AlterCommand::parse(const ASTAlterCommand * command_ast)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
    const CompressionCodecFactory & compression_codec_factory = CompressionCodecFactory::instance();

    if (command_ast->type == ASTAlterCommand::ADD_COLUMN)
    {
        AlterCommand command;
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
            command.codec = compression_codec_factory.get(ast_col_decl.codec, command.data_type);

        if (command_ast->column)
            command.after_column = getIdentifierName(command_ast->column);

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_COLUMN && !command_ast->partition)
    {
        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.type = AlterCommand::DROP_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        command.if_exists = command_ast->if_exists;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_COLUMN)
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_COLUMN;

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
            const auto & ast_comment = ast_col_decl.comment->as<ASTLiteral &>();
            command.comment.emplace(ast_comment.value.get<String>());
        }

        if (ast_col_decl.ttl)
            command.ttl = ast_col_decl.ttl;

        if (ast_col_decl.codec)
            command.codec = compression_codec_factory.get(ast_col_decl.codec, command.data_type);

        command.if_exists = command_ast->if_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::COMMENT_COLUMN)
    {
        AlterCommand command;
        command.type = COMMENT_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.get<String>();
        command.if_exists = command_ast->if_exists;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_ORDER_BY;
        command.order_by = command_ast->order_by;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_INDEX)
    {
        AlterCommand command;
        command.index_decl = command_ast->index_decl;
        command.type = AlterCommand::ADD_INDEX;

        const auto & ast_index_decl = command_ast->index_decl->as<ASTIndexDeclaration &>();

        command.index_name = ast_index_decl.name;

        if (command_ast->index)
            command.after_index_name = command_ast->index->as<ASTIdentifier &>().name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        AlterCommand command;
        command.constraint_decl = command_ast->constraint_decl;
        command.type = AlterCommand::ADD_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_CONSTRAINT && !command_ast->partition)
    {
        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.if_exists = command_ast->if_exists;
        command.type = AlterCommand::DROP_CONSTRAINT;
        command.constraint_name = command_ast->constraint->as<ASTIdentifier &>().name;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_INDEX && !command_ast->partition)
    {
        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR INDEX index\" queries are not supported yet. Use \"CLEAR INDEX index IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = command_ast->index->as<ASTIdentifier &>().name;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_TTL)
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_TTL;
        command.ttl = command_ast->ttl;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_SETTING)
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    }
    else if (command_ast->type == ASTAlterCommand::MODIFY_QUERY)
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_QUERY;
        command.select = command_ast->select;
        return command;
    }
    else
        return {};
}


void AlterCommand::apply(StorageInMemoryMetadata & metadata) const
{
    if (type == ADD_COLUMN)
    {
        ColumnDescription column(column_name, data_type, false);
        if (default_expression)
        {
            column.default_desc.kind = default_kind;
            column.default_desc.expression = default_expression;
        }
        if (comment)
            column.comment = *comment;

        column.codec = codec;
        column.ttl = ttl;

        metadata.columns.add(column, after_column);

        /// Slow, because each time a list is copied
        metadata.columns.flattenNested();
    }
    else if (type == DROP_COLUMN)
    {
        metadata.columns.remove(column_name);
    }
    else if (type == MODIFY_COLUMN)
    {
        metadata.columns.modify(column_name, [&](ColumnDescription & column)
        {
            if (codec)
            {
                /// User doesn't specify data type, it means that datatype doesn't change
                /// let's use info about old type
                if (data_type == nullptr)
                    codec->useInfoAboutType(column.type);
                else /// use info about new DataType
                    codec->useInfoAboutType(data_type);

                column.codec = codec;
            }

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
        });
    }
    else if (type == MODIFY_ORDER_BY)
    {
        if (!metadata.primary_key_ast && metadata.order_by_ast)
        {
            /// Primary and sorting key become independent after this ALTER so we have to
            /// save the old ORDER BY expression as the new primary key.
            metadata.primary_key_ast = metadata.order_by_ast->clone();
        }

        metadata.order_by_ast = order_by;
    }
    else if (type == COMMENT_COLUMN)
    {
        metadata.columns.modify(column_name, [&](ColumnDescription & column) { column.comment = *comment; });
    }
    else if (type == ADD_INDEX)
    {
        if (std::any_of(
                metadata.indices.indices.cbegin(),
                metadata.indices.indices.cend(),
                [this](const ASTPtr & index_ast)
                {
                    return index_ast->as<ASTIndexDeclaration &>().name == index_name;
                }))
        {
            if (if_not_exists)
                return;
            else
                throw Exception{"Cannot add index " + index_name + ": index with this name already exists",
                                ErrorCodes::ILLEGAL_COLUMN};
        }

        auto insert_it = metadata.indices.indices.end();

        if (!after_index_name.empty())
        {
            insert_it = std::find_if(
                    metadata.indices.indices.begin(),
                    metadata.indices.indices.end(),
                    [this](const ASTPtr & index_ast)
                    {
                        return index_ast->as<ASTIndexDeclaration &>().name == after_index_name;
                    });

            if (insert_it == metadata.indices.indices.end())
                throw Exception("Wrong index name. Cannot find index " + backQuote(after_index_name) + " to insert after.",
                        ErrorCodes::BAD_ARGUMENTS);

            ++insert_it;
        }

        metadata.indices.indices.emplace(insert_it, std::dynamic_pointer_cast<ASTIndexDeclaration>(index_decl));
    }
    else if (type == DROP_INDEX)
    {
        auto erase_it = std::find_if(
                metadata.indices.indices.begin(),
                metadata.indices.indices.end(),
                [this](const ASTPtr & index_ast)
                {
                    return index_ast->as<ASTIndexDeclaration &>().name == index_name;
                });

        if (erase_it == metadata.indices.indices.end())
        {
            if (if_exists)
                return;
            throw Exception("Wrong index name. Cannot find index " + backQuote(index_name) + " to drop.",
                            ErrorCodes::BAD_ARGUMENTS);
        }

        metadata.indices.indices.erase(erase_it);
    }
    else if (type == ADD_CONSTRAINT)
    {
        if (std::any_of(
                metadata.constraints.constraints.cbegin(),
                metadata.constraints.constraints.cend(),
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

        auto insert_it = metadata.constraints.constraints.end();

        metadata.constraints.constraints.emplace(insert_it, std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint_decl));
    }
    else if (type == DROP_CONSTRAINT)
    {
        auto erase_it = std::find_if(
                metadata.constraints.constraints.begin(),
                metadata.constraints.constraints.end(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                });

        if (erase_it == metadata.constraints.constraints.end())
        {
            if (if_exists)
                return;
            throw Exception("Wrong constraint name. Cannot find constraint `" + constraint_name + "` to drop.",
                    ErrorCodes::BAD_ARGUMENTS);
        }
        metadata.constraints.constraints.erase(erase_it);
    }
    else if (type == MODIFY_TTL)
    {
        metadata.ttl_for_table_ast = ttl;
    }
    else if (type == MODIFY_QUERY)
    {
        metadata.select = select;
    }
    else if (type == MODIFY_SETTING)
    {
        auto & settings_from_storage = metadata.settings_ast->as<ASTSetQuery &>().changes;
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
    else
        throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
}

bool AlterCommand::isModifyingData() const
{
    /// Possible change data representation on disk
    if (type == MODIFY_COLUMN)
        return data_type != nullptr;

    return type == ADD_COLUMN  /// We need to change columns.txt in each part for MergeTree
        || type == DROP_COLUMN /// We need to change columns.txt in each part for MergeTree
        || type == DROP_INDEX; /// We need to remove file from filesystem for MergeTree
}

bool AlterCommand::isSettingsAlter() const
{
    return type == MODIFY_SETTING;
}

bool AlterCommand::isCommentAlter() const
{
    if (type == COMMENT_COLUMN)
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


String alterTypeToString(const AlterCommand::Type type)
{
    switch (type)
    {
    case AlterCommand::Type::ADD_COLUMN:
        return "ADD COLUMN";
    case AlterCommand::Type::ADD_CONSTRAINT:
        return "ADD CONSTRAINT";
    case AlterCommand::Type::ADD_INDEX:
        return "ADD INDEX";
    case AlterCommand::Type::COMMENT_COLUMN:
        return "COMMENT COLUMN";
    case AlterCommand::Type::DROP_COLUMN:
        return "DROP COLUMN";
    case AlterCommand::Type::DROP_CONSTRAINT:
        return "DROP CONSTRAINT";
    case AlterCommand::Type::DROP_INDEX:
        return "DROP INDEX";
    case AlterCommand::Type::MODIFY_COLUMN:
        return "MODIFY COLUMN";
    case AlterCommand::Type::MODIFY_ORDER_BY:
        return "MODIFY ORDER BY";
    case AlterCommand::Type::MODIFY_TTL:
        return "MODIFY TTL";
    case AlterCommand::Type::MODIFY_SETTING:
        return "MODIFY SETTING";
    case AlterCommand::Type::MODIFY_QUERY:
        return "MODIFY QUERY";
    }
    __builtin_unreachable();
}

void AlterCommands::apply(StorageInMemoryMetadata & metadata) const
{
    if (!prepared)
        throw DB::Exception("Alter commands is not prepared. Cannot apply. It's a bug", ErrorCodes::LOGICAL_ERROR);

    auto metadata_copy = metadata;
    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(metadata_copy);

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

            if (has_column && command.data_type)
            {
                auto column_from_table = columns.get(command.column_name);
                if (!command.default_expression && column_from_table.default_desc.expression)
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
                || command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!has_column && command.if_exists)
                command.ignore = true;
        }
    }
    prepared = true;
}

void AlterCommands::validate(const StorageInMemoryMetadata & metadata, const Context & context) const
{
    auto all_columns = metadata.columns;
    for (size_t i = 0; i < size(); ++i)
    {
        auto & command = (*this)[i];

        const auto & column_name = command.column_name;
        if (command.type == AlterCommand::ADD_COLUMN)
        {
            if (metadata.columns.has(column_name) || metadata.columns.hasNested(column_name))
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

            if (command.default_expression)
                validateDefaultExpressionForColumn(command.default_expression, column_name, command.data_type, all_columns, context);

            all_columns.add(ColumnDescription(column_name, command.data_type, false));
        }
        else if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            if (!metadata.columns.has(column_name))
            {
                if (!command.if_exists)
                    throw Exception{"Wrong column name. Cannot find column " + backQuote(column_name) + " to modify",
                                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK};
                else
                    continue;
            }

            auto column_in_table = metadata.columns.get(column_name);
            if (command.default_expression)
            {
                if (!command.data_type)
                    validateDefaultExpressionForColumn(
                        command.default_expression, column_name, column_in_table.type, all_columns, context);
                else
                    validateDefaultExpressionForColumn(
                        command.default_expression, column_name, command.data_type, all_columns, context);
            }
            else if (column_in_table.default_desc.expression && command.data_type)
            {
                validateDefaultExpressionForColumn(
                    column_in_table.default_desc.expression, column_name, command.data_type, all_columns, context);
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (metadata.columns.has(command.column_name) || metadata.columns.hasNested(command.column_name))
            {
                for (const ColumnDescription & column : metadata.columns)
                {
                    const auto & default_expression = column.default_desc.expression;
                    if (default_expression)
                    {
                        ASTPtr query = default_expression->clone();
                        auto syntax_result = SyntaxAnalyzer(context).analyze(query, metadata.columns.getAll());
                        const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
                        const auto required_columns = actions->getRequiredColumns();

                        if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                            throw Exception(
                                "Cannot drop column " + backQuote(command.column_name) + ", because column " + backQuote(column.name) + " depends on it",
                                ErrorCodes::ILLEGAL_COLUMN);
                    }
                }
            }
            else if (!command.if_exists)
                throw Exception(
                    "Wrong column name. Cannot find column " + backQuote(command.column_name) + " to drop",
                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!metadata.columns.has(command.column_name))
            {
                if (!command.if_exists)
                    throw Exception{"Wrong column name. Cannot find column " + backQuote(command.column_name) + " to comment",
                                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK};
            }
        }
    }
}

void AlterCommands::validateDefaultExpressionForColumn(
    const ASTPtr default_expression,
    const String & column_name,
    const DataTypePtr column_type,
    const ColumnsDescription & all_columns,
    const Context & context) const
{

    try
    {
        String tmp_column_name = "__tmp" + column_name;
        auto copy_expression = default_expression->clone();
        auto default_with_cast = makeASTFunction("CAST", copy_expression, std::make_shared<ASTLiteral>(column_type->getName()));
        auto query_with_alias = setAlias(default_with_cast, tmp_column_name);
        auto syntax_result = SyntaxAnalyzer(context).analyze(query_with_alias, all_columns.getAll());
        ExpressionAnalyzer(query_with_alias, syntax_result, context).getActions(true);
    }
    catch (Exception & ex)
    {
        ex.addMessage("default expression and column type are incompatible. Cannot alter column " + backQuote(column_name));
        throw;
    }
}

bool AlterCommands::isModifyingData() const
{
    for (const auto & param : *this)
    {
        if (param.isModifyingData())
            return true;
    }

    return false;
}

bool AlterCommands::isSettingsAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });
}

bool AlterCommands::isCommentAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isCommentAlter(); });
}
}
