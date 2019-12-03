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

    auto parse_add_column = [&]() -> std::optional<AlterCommand>
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
    };

    auto parse_drop_column = [&]() -> std::optional<AlterCommand>
    {
        if (command_ast->partition)
            return {};

        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.type = AlterCommand::DROP_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        command.if_exists = command_ast->if_exists;
        return command;
    };

    auto parse_modify_column = [&]() -> std::optional<AlterCommand>
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
    };

    auto parse_comment_column = [&]() -> std::optional<AlterCommand>
    {
        AlterCommand command;
        command.type = COMMENT_COLUMN;
        command.column_name = getIdentifierName(command_ast->column);
        const auto & ast_comment = command_ast->comment->as<ASTLiteral &>();
        command.comment = ast_comment.value.get<String>();
        command.if_exists = command_ast->if_exists;
        return command;
    };

    auto parse_modify_order_by = [&]() -> std::optional<AlterCommand>
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_ORDER_BY;
        command.order_by = command_ast->order_by;
        return command;
    };

    auto parse_add_index = [&]() -> std::optional<AlterCommand>
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
    };

    auto parse_add_constraint = [&]() -> std::optional<AlterCommand>
    {
        AlterCommand command;
        command.constraint_decl = command_ast->constraint_decl;
        command.type = AlterCommand::ADD_CONSTRAINT;

        const auto & ast_constraint_decl = command_ast->constraint_decl->as<ASTConstraintDeclaration &>();

        command.constraint_name = ast_constraint_decl.name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    };

    auto parse_drop_constraint = [&]() -> std::optional<AlterCommand>
    {
        if (command_ast->partition)
            return {};

        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.if_exists = command_ast->if_exists;
        command.type = AlterCommand::DROP_CONSTRAINT;
        command.constraint_name = command_ast->constraint->as<ASTIdentifier &>().name;

        return command;
    };

    auto parse_drop_index = [&]() -> std::optional<AlterCommand>
    {
        if (command_ast->partition)
            return {};

        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR INDEX index\" queries are not supported yet. Use \"CLEAR INDEX index IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = command_ast->index->as<ASTIdentifier &>().name;
        command.if_exists = command_ast->if_exists;

        return command;
    };

    auto parse_modify_ttl = [&]() -> std::optional<AlterCommand>
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_TTL;
        command.ttl = command_ast->ttl;
        return command;
    };

    auto parse_modify_setting = [&]() -> std::optional<AlterCommand>
    {
        AlterCommand command;
        command.type = AlterCommand::MODIFY_SETTING;
        command.settings_changes = command_ast->settings_changes->as<ASTSetQuery &>().changes;
        return command;
    };

    switch (command_ast->type)
    {
        case ASTAlterCommand::ADD_COLUMN: return parse_add_column();
        case ASTAlterCommand::DROP_COLUMN: return parse_drop_column();
        case ASTAlterCommand::MODIFY_COLUMN: return parse_modify_column();
        case ASTAlterCommand::COMMENT_COLUMN: return parse_comment_column();
        case ASTAlterCommand::MODIFY_ORDER_BY: return parse_modify_order_by();
        case ASTAlterCommand::ADD_INDEX: return parse_add_index();
        case ASTAlterCommand::ADD_CONSTRAINT: return parse_add_constraint();
        case ASTAlterCommand::DROP_CONSTRAINT: return parse_drop_constraint();
        case ASTAlterCommand::DROP_INDEX: return parse_drop_index();
        case ASTAlterCommand::MODIFY_TTL: return parse_modify_ttl();
        case ASTAlterCommand::MODIFY_SETTING: return parse_modify_setting();
        default: return {};
    }
}


void AlterCommand::apply(ApplyDescriptions & descriptions, ApplySupportingASTPtrs & astPtrs, SettingsChanges & changes) const
{
    auto columns_description = descriptions.columns ? *descriptions.columns : ColumnsDescription();
    auto indices_description = descriptions.indices ? *descriptions.indices : IndicesDescription();
    auto constraints_description = descriptions.constraints ? *descriptions.constraints : ConstraintsDescription();

    auto & order_by_ast = astPtrs.order_by;
    auto & primary_key_ast = astPtrs.primary_key;
    auto & ttl_table_ast = astPtrs.ttl_table;

    auto apply_add_column = [&]()
    {
        ColumnDescription column(column_name, data_type, false);
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
    };

    auto apply_drop_column = [&]()
    {
        columns_description.remove(column_name);
    };

    auto apply_modify_column = [&]()
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
    };

    auto apply_modify_order_by = [&]()
    {
        if (!primary_key_ast && order_by_ast)
        {
            /// Primary and sorting key become independent after this ALTER so we have to
            /// save the old ORDER BY expression as the new primary key.
            primary_key_ast = order_by_ast->clone();
        }

        order_by_ast = order_by;
    };

    auto apply_comment_column = [&]()
    {
        columns_description.modify(column_name, [&](ColumnDescription & column) { column.comment = comment; });
    };

    auto apply_add_index = [&]()
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
                throw Exception("Wrong index name. Cannot find index " + backQuote(after_index_name) + " to insert after.",
                                ErrorCodes::LOGICAL_ERROR);

            ++insert_it;
        }

        indices_description.indices.emplace(insert_it, std::dynamic_pointer_cast<ASTIndexDeclaration>(index_decl));
    };

    auto apply_drop_index = [&]()
    {
        auto erase_it = std::find_if(
                indices_description.indices.begin(),
                indices_description.indices.end(),
                [this](const ASTPtr & index_ast)
                {
                    return index_ast->as<ASTIndexDeclaration &>().name == index_name;
                });

        if (erase_it == indices_description.indices.end())
        {
            if (if_exists)
                return;
            throw Exception("Wrong index name. Cannot find index " + backQuote(index_name) + " to drop.",
                            ErrorCodes::LOGICAL_ERROR);
        }

        indices_description.indices.erase(erase_it);
    };

    auto apply_add_constraint = [&]()
    {
        if (std::any_of(
                constraints_description.constraints.cbegin(),
                constraints_description.constraints.cend(),
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

        auto insert_it = constraints_description.constraints.end();

        constraints_description.constraints.emplace(insert_it, std::dynamic_pointer_cast<ASTConstraintDeclaration>(constraint_decl));
    };


    auto apply_drop_constraint = [&]()
    {
        auto erase_it = std::find_if(
                constraints_description.constraints.begin(),
                constraints_description.constraints.end(),
                [this](const ASTPtr & constraint_ast)
                {
                    return constraint_ast->as<ASTConstraintDeclaration &>().name == constraint_name;
                });

        if (erase_it == constraints_description.constraints.end())
        {
            if (if_exists)
                return;
            throw Exception("Wrong constraint name. Cannot find constraint `" + constraint_name + "` to drop.",
                            ErrorCodes::LOGICAL_ERROR);
        }
        constraints_description.constraints.erase(erase_it);
    };

    auto apply_modify_ttl = [&]()
    {
        ttl_table_ast = ttl;
    };

    auto apply_modify_setting = [&]()
    {
        changes.insert(changes.end(), settings_changes.begin(), settings_changes.end());
    };

    switch (type)
    {
        case ADD_COLUMN: apply_add_column(); break;
        case DROP_COLUMN: apply_drop_column(); break;
        case MODIFY_COLUMN: apply_modify_column(); break;
        case MODIFY_ORDER_BY: apply_modify_order_by(); break;
        case COMMENT_COLUMN: apply_comment_column(); break;
        case ADD_INDEX: apply_add_index(); break;
        case DROP_INDEX: apply_drop_index(); break;
        case ADD_CONSTRAINT: apply_add_constraint(); break;
        case DROP_CONSTRAINT: apply_drop_constraint(); break;
        case MODIFY_TTL: apply_modify_ttl(); break;
        case MODIFY_SETTING: apply_modify_setting(); break;
        default: throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }
}



void AlterCommand::apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
    ConstraintsDescription & constraints_description, ASTPtr & order_by_ast, ASTPtr & primary_key_ast,
    ASTPtr & ttl_table_ast, SettingsChanges & changes) const
{
    ApplyDescriptions descriptions{columns_description, indices_description, constraints_description};
    ApplySupportingASTPtrs ast{order_by_ast, ttl_table_ast, primary_key_ast};
    apply(descriptions, ast, changes);
}

bool AlterCommand::isMutable() const
{
    if (type == COMMENT_COLUMN || type == MODIFY_SETTING)
        return false;
    if (type == MODIFY_COLUMN)
        return data_type.get() || default_expression;
    return true;
}

bool AlterCommand::isSettingsAlter() const
{
    return type == MODIFY_SETTING;
}

void AlterCommands::apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
    ConstraintsDescription & constraints_description, ASTPtr & order_by_ast, ASTPtr & primary_key_ast,
    ASTPtr & ttl_table_ast, SettingsChanges & changes) const
{
    ApplyDescriptions new_descriptions{columns_description, indices_description, constraints_description};
    AlterCommand::ApplySupportingASTPtrs new_astPtrs{order_by_ast, ttl_table_ast, primary_key_ast};
    auto new_changes = changes;

    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(new_descriptions, new_astPtrs, new_changes);

    columns_description = std::move(*new_descriptions.columns);
    indices_description = std::move(*new_descriptions.indices);
    constraints_description = std::move(*new_descriptions.constraints);
    order_by_ast = std::move(new_astPtrs.order_by);
    primary_key_ast = std::move(new_astPtrs.primary_key);
    ttl_table_ast = std::move(new_astPtrs.ttl_table);
    changes = std::move(new_changes);
}

void AlterCommands::apply(ApplyDescriptions & descriptions, ApplySupportingASTPtrs & astPtrs, SettingsChanges & changes) const
{
    auto new_descriptions = descriptions;
    auto new_astPtrs = astPtrs;
    auto new_changes = changes;

    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(descriptions, astPtrs, changes);

    descriptions = std::move(new_descriptions);
    astPtrs = std::move(new_astPtrs);
    changes = std::move(new_changes);
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
        else if (command.type == AlterCommand::MODIFY_SETTING)
            for (const auto & change : command.settings_changes)
                table.checkSettingCanBeChanged(change.name);
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

void AlterCommands::applyForColumnsOnly(ColumnsDescription & columns_description) const
{
    ApplyDescriptions descriptions;
    descriptions.columns = std::make_shared<ColumnsDescription>(columns_description);
    ApplySupportingASTPtrs out_astPtrs;
    SettingsChanges out_changes;

    apply(descriptions, out_astPtrs, out_changes);

    if (descriptions.indices && !descriptions.indices->indices.empty())
        throw Exception("Storage doesn't support modifying indices", ErrorCodes::NOT_IMPLEMENTED);
    if (descriptions.constraints && !descriptions.constraints->constraints.empty())
        throw Exception("Storage doesn't support modifying constraints", ErrorCodes::NOT_IMPLEMENTED);
    if (out_astPtrs.order_by)
        throw Exception("Storage doesn't support modifying ORDER BY expression", ErrorCodes::NOT_IMPLEMENTED);
    if (out_astPtrs.primary_key)
        throw Exception("Storage doesn't support modifying PRIMARY KEY expression", ErrorCodes::NOT_IMPLEMENTED);
    if (out_astPtrs.ttl_table)
        throw Exception("Storage doesn't support modifying TTL expression", ErrorCodes::NOT_IMPLEMENTED);
    if (!out_changes.empty())
        throw Exception("Storage doesn't support modifying settings", ErrorCodes::NOT_IMPLEMENTED);

    columns_description = std::move(*descriptions.columns);
}


void AlterCommands::applyForSettingsOnly(SettingsChanges & changes) const
{
    ApplyDescriptions out_descriptions;
    ApplySupportingASTPtrs out_astPtrs;
    SettingsChanges out_changes;

    apply(out_descriptions, out_astPtrs, out_changes);

    if (out_descriptions.columns && out_descriptions.columns->begin() != out_descriptions.columns->end())
        throw Exception("Alter modifying columns, but only settings change applied.", ErrorCodes::LOGICAL_ERROR);
    if (out_descriptions.indices && !out_descriptions.indices->indices.empty())
        throw Exception("Alter modifying indices, but only settings change applied.", ErrorCodes::NOT_IMPLEMENTED);
    if (out_astPtrs.order_by)
        throw Exception("Alter modifying ORDER BY expression, but only settings change applied.", ErrorCodes::LOGICAL_ERROR);
    if (out_astPtrs.primary_key)
        throw Exception("Alter modifying PRIMARY KEY expression, but only settings change applied.", ErrorCodes::LOGICAL_ERROR);
    if (out_astPtrs.ttl_table)
        throw Exception("Alter modifying TTL, but only settings change applied.", ErrorCodes::NOT_IMPLEMENTED);

    changes = std::move(out_changes);
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

bool AlterCommands::isSettingsAlter() const
{
    return std::all_of(begin(), end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });
}
}
