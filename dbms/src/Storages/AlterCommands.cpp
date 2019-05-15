#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/SyntaxAnalyzer.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionFactory.h>

#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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
            command.after_column = *getIdentifierName(command_ast->column);

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
        command.column_name = *getIdentifierName(command_ast->column);
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
            command.comment = ast_comment.value.get<String>();
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
        command.column_name = *getIdentifierName(command_ast->column);
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
    else if (command_ast->type == ASTAlterCommand::DROP_INDEX)
    {
        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

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
    else
        return {};
}


void AlterCommand::apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
        ASTPtr & order_by_ast, ASTPtr & primary_key_ast, ASTPtr & ttl_table_ast) const
{
    if (type == ADD_COLUMN)
    {
        ColumnDescription column(column_name, data_type);
        if (default_expression)
        {
            column.default_desc.kind = default_kind;
            column.default_desc.expression = default_expression;
        }
        column.comment = comment;
        column.codec = codec;
        column.ttl = ttl;

        columns_description.add(column, after_column);

        /// Slow, because each time a list is copied
        columns_description.flattenNested();
    }
    else if (type == DROP_COLUMN)
    {
        columns_description.remove(column_name);
    }
    else if (type == MODIFY_COLUMN)
    {
        columns_description.modify(column_name, [&](ColumnDescription & column)
        {
            if (codec)
            {
                /// User doesn't specify data type, it means that datatype doesn't change
                /// let's use info about old type
                if (data_type == nullptr)
                    codec->useInfoAboutType(column.type);
                column.codec = codec;
            }

            if (!isMutable())
            {
                column.comment = comment;
                return;
            }

            if (ttl)
                column.ttl = ttl;

            column.type = data_type;

            column.default_desc.kind = default_kind;
            column.default_desc.expression = default_expression;
        });
    }
    else if (type == MODIFY_ORDER_BY)
    {
        if (!primary_key_ast && order_by_ast)
        {
            /// Primary and sorting key become independent after this ALTER so we have to
            /// save the old ORDER BY expression as the new primary key.
            primary_key_ast = order_by_ast->clone();
        }

        order_by_ast = order_by;
    }
    else if (type == COMMENT_COLUMN)
    {
        columns_description.modify(column_name, [&](ColumnDescription & column) { column.comment = comment; });
    }
    else if (type == ADD_INDEX)
    {
        if (std::any_of(
                indices_description.indices.cbegin(),
                indices_description.indices.cend(),
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

        auto insert_it = indices_description.indices.end();

        if (!after_index_name.empty())
        {
            insert_it = std::find_if(
                    indices_description.indices.begin(),
                    indices_description.indices.end(),
                    [this](const ASTPtr & index_ast)
                    {
                        return index_ast->as<ASTIndexDeclaration &>().name == after_index_name;
                    });

            if (insert_it == indices_description.indices.end())
                throw Exception("Wrong index name. Cannot find index `" + after_index_name + "` to insert after.",
                        ErrorCodes::LOGICAL_ERROR);

            ++insert_it;
        }

        indices_description.indices.emplace(insert_it, std::dynamic_pointer_cast<ASTIndexDeclaration>(index_decl));
    }
    else if (type == DROP_INDEX)
    {
        auto erase_it = std::find_if(
                indices_description.indices.begin(),
                indices_description.indices.end(),
                [this](const ASTPtr & index_ast)
                {
                    return index_ast->as<ASTIndexDeclaration &>().name == index_name;
                });

        if (erase_it == indices_description.indices.end())
            throw Exception("Wrong index name. Cannot find index `" + index_name + "` to drop.",
                    ErrorCodes::LOGICAL_ERROR);

        indices_description.indices.erase(erase_it);
    }
    else if (type == MODIFY_TTL)
    {
        ttl_table_ast = ttl;
    }
    else
        throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
}

bool AlterCommand::isMutable() const
{
    if (type == COMMENT_COLUMN)
        return false;
    if (type == MODIFY_COLUMN)
        return data_type.get() || default_expression;
    // TODO: возможно, здесь нужно дополнить
    return true;
}

void AlterCommands::apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
        ASTPtr & order_by_ast, ASTPtr & primary_key_ast, ASTPtr & ttl_table_ast) const
{
    auto new_columns_description = columns_description;
    auto new_indices_description = indices_description;
    auto new_order_by_ast = order_by_ast;
    auto new_primary_key_ast = primary_key_ast;
    auto new_ttl_table_ast = ttl_table_ast;

    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(new_columns_description, new_indices_description, new_order_by_ast, new_primary_key_ast, new_ttl_table_ast);

    columns_description = std::move(new_columns_description);
    indices_description = std::move(new_indices_description);
    order_by_ast = std::move(new_order_by_ast);
    primary_key_ast = std::move(new_primary_key_ast);
    ttl_table_ast = std::move(new_ttl_table_ast);
}

void AlterCommands::validate(const IStorage & table, const Context & context)
{
    /// A temporary object that is used to keep track of the current state of columns after applying a subset of commands.
    auto columns = table.getColumns();

    /// Default expressions will be added to this list for type deduction.
    auto default_expr_list = std::make_shared<ASTExpressionList>();
    /// We will save ALTER ADD/MODIFY command indices (only the last for each column) for possible modification
    /// (we might need to add deduced types or modify default expressions).
    /// Saving indices because we can add new commands later and thus cause vector resize.
    std::unordered_map<String, size_t> column_to_command_idx;

    for (size_t i = 0; i < size(); ++i)
    {
        auto & command = (*this)[i];
        if (command.type == AlterCommand::ADD_COLUMN || command.type == AlterCommand::MODIFY_COLUMN)
        {
            const auto & column_name = command.column_name;

            if (command.type == AlterCommand::ADD_COLUMN)
            {
                if (columns.has(column_name) || columns.hasNested(column_name))
                {
                    if (command.if_not_exists)
                        command.ignore = true;
                    else
                        throw Exception{"Cannot add column " + column_name + ": column with this name already exists", ErrorCodes::ILLEGAL_COLUMN};
                }
            }
            else if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (!columns.has(column_name))
                {
                    if (command.if_exists)
                        command.ignore = true;
                    else
                        throw Exception{"Wrong column name. Cannot find column " + column_name + " to modify", ErrorCodes::ILLEGAL_COLUMN};
                }

                if (!command.ignore)
                    columns.remove(column_name);
            }

            if (!command.ignore)
            {
                column_to_command_idx[column_name] = i;

                /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
                columns.add(ColumnDescription(
                    column_name, command.data_type ? command.data_type : std::make_shared<DataTypeUInt8>()));

                if (command.default_expression)
                {
                    if (command.data_type)
                    {
                        const auto & final_column_name = column_name;
                        const auto tmp_column_name = final_column_name + "_tmp";

                        default_expr_list->children.emplace_back(setAlias(
                            makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                                std::make_shared<ASTLiteral>(command.data_type->getName())),
                            final_column_name));

                        default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), tmp_column_name));
                    }
                    else
                    {
                        /// no type explicitly specified, will deduce later
                        default_expr_list->children.emplace_back(
                            setAlias(command.default_expression->clone(), column_name));
                    }
                }
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (columns.has(command.column_name) || columns.hasNested(command.column_name))
            {
                for (const ColumnDescription & column : columns)
                {
                    const auto & default_expression = column.default_desc.expression;
                    if (!default_expression)
                        continue;

                    ASTPtr query = default_expression->clone();
                    auto syntax_result = SyntaxAnalyzer(context).analyze(query, columns.getAll());
                    const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
                    const auto required_columns = actions->getRequiredColumns();

                    if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                        throw Exception(
                            "Cannot drop column " + command.column_name + ", because column " + column.name +
                            " depends on it", ErrorCodes::ILLEGAL_COLUMN);
                }

                columns.remove(command.column_name);
            }
            else if (command.if_exists)
                command.ignore = true;
            else
                throw Exception("Wrong column name. Cannot find column " + command.column_name + " to drop",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!columns.has(command.column_name))
            {
                if (command.if_exists)
                    command.ignore = true;
                else
                    throw Exception{"Wrong column name. Cannot find column " + command.column_name + " to comment", ErrorCodes::ILLEGAL_COLUMN};
            }
        }
    }

    /** Existing defaulted columns may require default expression extensions with a type conversion,
        *  therefore we add them to default_expr_list to recalculate their types */
    for (const auto & column : columns)
    {
        if (column.default_desc.expression)
        {
            const auto tmp_column_name = column.name + "_tmp";

            default_expr_list->children.emplace_back(setAlias(
                    makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                        std::make_shared<ASTLiteral>(column.type->getName())),
                    column.name));

            default_expr_list->children.emplace_back(setAlias(column.default_desc.expression->clone(), tmp_column_name));
        }
    }

    ASTPtr query = default_expr_list;
    auto syntax_result = SyntaxAnalyzer(context).analyze(query, columns.getAll());
    const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
    const auto block = actions->getSampleBlock();

    /// set deduced types, modify default expression if necessary
    for (const auto & column : columns)
    {
        AlterCommand * command = nullptr;
        auto command_it = column_to_command_idx.find(column.name);
        if (command_it != column_to_command_idx.end())
            command = &(*this)[command_it->second];

        if (!(command && command->default_expression) && !column.default_desc.expression)
            continue;

        const DataTypePtr & explicit_type = command ? command->data_type : column.type;
        if (explicit_type)
        {
            const auto & tmp_column = block.getByName(column.name + "_tmp");
            const auto & deduced_type = tmp_column.type;
            if (!explicit_type->equals(*deduced_type))
            {
                if (!command)
                {
                    /// column has no associated alter command, let's create it
                    /// add a new alter command to modify existing column
                    this->emplace_back(AlterCommand{AlterCommand::MODIFY_COLUMN,
                        column.name, explicit_type, column.default_desc.kind, column.default_desc.expression, {}, {}, {}, {}});

                    command = &back();
                }

                command->default_expression = makeASTFunction("CAST",
                    command->default_expression->clone(),
                    std::make_shared<ASTLiteral>(explicit_type->getName()));
            }
        }
        else
        {
            /// just set deduced type
            command->data_type = block.getByName(column.name).type;
        }
    }
}

void AlterCommands::apply(ColumnsDescription & columns_description) const
{
    auto out_columns_description = columns_description;
    IndicesDescription indices_description;
    ASTPtr out_order_by;
    ASTPtr out_primary_key;
    ASTPtr out_ttl_table;
    apply(out_columns_description, indices_description, out_order_by, out_primary_key, out_ttl_table);

    if (out_order_by)
        throw Exception("Storage doesn't support modifying ORDER BY expression", ErrorCodes::NOT_IMPLEMENTED);
    if (out_primary_key)
        throw Exception("Storage doesn't support modifying PRIMARY KEY expression", ErrorCodes::NOT_IMPLEMENTED);
    if (!indices_description.indices.empty())
        throw Exception("Storage doesn't support modifying indices", ErrorCodes::NOT_IMPLEMENTED);
    if (out_ttl_table)
        throw Exception("Storage doesn't support modifying TTL expression", ErrorCodes::NOT_IMPLEMENTED);

    columns_description = std::move(out_columns_description);
}

bool AlterCommands::isMutable() const
{
    for (const auto & param : *this)
    {
        if (param.isMutable())
            return true;
    }

    return false;
}

}
