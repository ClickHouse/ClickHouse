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
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
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
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_SETTING;
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
                        ErrorCodes::LOGICAL_ERROR);

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
                            ErrorCodes::LOGICAL_ERROR);
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
                    ErrorCodes::LOGICAL_ERROR);
        }
        metadata.constraints.constraints.erase(erase_it);
    }
    else if (type == MODIFY_TTL)
    {
        metadata.ttl_for_table_ast = ttl;
    }
    else if (type == MODIFY_SETTING)
    {
        auto & settings_from_storage = metadata.settings_ast->as<ASTSetQuery &>().changes;
        for (const auto & change : settings_changes)
        {
            auto finder = [&change](const SettingChange & c) { return c.name == change.name; };
            if (auto it = std::find_if(settings_from_storage.begin(), settings_from_storage.end(), finder);
                it != settings_from_storage.end())
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


void AlterCommands::prepare(const StorageInMemoryMetadata & metadata, const Context & context)
{
    /// A temporary object that is used to keep track of the current state of columns after applying a subset of commands.
    auto columns = metadata.columns;

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
                }
            }
            else if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (!columns.has(column_name))
                    if (command.if_exists)
                        command.ignore = true;

                if (!command.ignore)
                    columns.remove(column_name);
            }

            if (!command.ignore)
            {
                column_to_command_idx[column_name] = i;

                /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
                columns.add(
                    ColumnDescription(column_name, command.data_type ? command.data_type : std::make_shared<DataTypeUInt8>(), false));

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
                columns.remove(command.column_name);
            else if (command.if_exists)
                command.ignore = true;
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!columns.has(command.column_name) && command.if_exists)
                command.ignore = true;
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
                    AlterCommand aux_command{
                        .type = AlterCommand::MODIFY_COLUMN,
                        .column_name = column.name,
                        .data_type = explicit_type,
                        .default_kind = column.default_desc.kind,
                        .default_expression = column.default_desc.expression
                    };
                    /// column has no associated alter command, let's create it
                    /// add a new alter command to modify existing column
                    this->emplace_back(aux_command);

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
    prepared = true;
}

void AlterCommands::validate(const StorageInMemoryMetadata & metadata, const Context & context) const
{
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
                if (metadata.columns.has(column_name) || metadata.columns.hasNested(column_name))
                    if (!command.if_not_exists)
                        throw Exception{"Cannot add column " + column_name + ": column with this name already exists", ErrorCodes::ILLEGAL_COLUMN};
            }
            else if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (!metadata.columns.has(column_name))
                    if (!command.if_exists)
                        throw Exception{"Wrong column name. Cannot find column " + column_name + " to modify", ErrorCodes::ILLEGAL_COLUMN};
            }

        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (metadata.columns.has(command.column_name) || metadata.columns.hasNested(command.column_name))
            {
                for (const ColumnDescription & column : metadata.columns)
                {
                    const auto & default_expression = column.default_desc.expression;
                    if (!default_expression)
                        continue;

                    ASTPtr query = default_expression->clone();
                    auto syntax_result = SyntaxAnalyzer(context).analyze(query, metadata.columns.getAll());
                    const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
                    const auto required_columns = actions->getRequiredColumns();

                    if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                        throw Exception(
                            "Cannot drop column " + command.column_name + ", because column " + column.name +
                            " depends on it", ErrorCodes::ILLEGAL_COLUMN);
                }
            }
            else if (!command.if_exists)
                throw Exception("Wrong column name. Cannot find column " + command.column_name + " to drop",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            if (!metadata.columns.has(command.column_name))
            {
                if (!command.if_exists)
                    throw Exception{"Wrong column name. Cannot find column " + command.column_name + " to comment", ErrorCodes::ILLEGAL_COLUMN};
            }
        }
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
