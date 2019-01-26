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

        const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*command_ast->col_decl);

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

        if (ast_col_decl.codec)
            command.codec = compression_codec_factory.get(ast_col_decl.codec);

        if (command_ast->column)
            command.after_column = *getIdentifierName(command_ast->column);

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

        const auto & ast_col_decl = typeid_cast<const ASTColumnDeclaration &>(*command_ast->col_decl);

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

        if (ast_col_decl.codec)
            command.codec = compression_codec_factory.get(ast_col_decl.codec);

        if (ast_col_decl.comment)
        {
            const auto & ast_comment = typeid_cast<ASTLiteral &>(*ast_col_decl.comment);
            command.comment = ast_comment.value.get<String>();
        }
        command.if_exists = command_ast->if_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::COMMENT_COLUMN)
    {
        AlterCommand command;
        command.type = COMMENT_COLUMN;
        command.column_name = *getIdentifierName(command_ast->column);
        const auto & ast_comment = typeid_cast<ASTLiteral &>(*command_ast->comment);
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

        const auto & ast_index_decl = typeid_cast<const ASTIndexDeclaration &>(*command_ast->index_decl);

        command.index_name = ast_index_decl.name;

        if (command_ast->index)
            command.after_index_name = typeid_cast<const ASTIdentifier &>(*command_ast->index).name;

        command.if_not_exists = command_ast->if_not_exists;

        return command;
    }
    else if (command_ast->type == ASTAlterCommand::DROP_INDEX)
    {
        if (command_ast->clear_column)
            throw Exception("\"ALTER TABLE table CLEAR COLUMN column\" queries are not supported yet. Use \"CLEAR COLUMN column IN PARTITION\".", ErrorCodes::NOT_IMPLEMENTED);

        AlterCommand command;
        command.type = AlterCommand::DROP_INDEX;
        command.index_name = typeid_cast<const ASTIdentifier &>(*(command_ast->index)).name;
        command.if_exists = command_ast->if_exists;

        return command;
    }
    else
        return {};
}


/// the names are the same if they match the whole name or name_without_dot matches the part of the name up to the dot
static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
{
    String name_with_dot = name_without_dot + ".";
    return (name_with_dot == name_type.name.substr(0, name_without_dot.length() + 1) || name_without_dot == name_type.name);
}

void AlterCommand::apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
        ASTPtr & order_by_ast, ASTPtr & primary_key_ast) const
{
    if (type == ADD_COLUMN)
    {
        if (columns_description.getAll().contains(column_name))
            throw Exception{"Cannot add column " + column_name + ": column with this name already exists", ErrorCodes::ILLEGAL_COLUMN};

        const auto add_column = [this] (NamesAndTypesList & columns)
        {
            auto insert_it = columns.end();

            if (!after_column.empty())
            {
                /// We are trying to find first column from end with name `column_name` or with a name beginning with `column_name` and ".".
                /// For example "fruits.bananas"
                /// names are considered the same if they completely match or `name_without_dot` matches the part of the name to the point
                const auto reverse_insert_it = std::find_if(columns.rbegin(), columns.rend(),
                    std::bind(namesEqual, std::cref(after_column), std::placeholders::_1));

                if (reverse_insert_it == columns.rend())
                    throw Exception("Wrong column name. Cannot find column " + after_column + " to insert after",
                                    ErrorCodes::ILLEGAL_COLUMN);
                else
                {
                    /// base returns an iterator that is already offset by one element to the right
                    insert_it = reverse_insert_it.base();
                }
            }

            columns.emplace(insert_it, column_name, data_type);
        };

        if (default_kind == ColumnDefaultKind::Default)
            add_column(columns_description.ordinary);
        else if (default_kind == ColumnDefaultKind::Materialized)
            add_column(columns_description.materialized);
        else if (default_kind == ColumnDefaultKind::Alias)
            add_column(columns_description.aliases);
        else
            throw Exception{"Unknown ColumnDefaultKind value", ErrorCodes::LOGICAL_ERROR};

        if (default_expression)
            columns_description.defaults.emplace(column_name, ColumnDefault{default_kind, default_expression});

        if (codec)
            columns_description.codecs.emplace(column_name, codec);

        /// Slow, because each time a list is copied
        columns_description.ordinary = Nested::flatten(columns_description.ordinary);
    }
    else if (type == DROP_COLUMN)
    {
        /// look for a column in list and remove it if present, also removing corresponding entry from column_defaults
        const auto remove_column = [&columns_description, this] (NamesAndTypesList & columns)
        {
            auto removed = false;
            NamesAndTypesList::iterator column_it;

            while (columns.end() != (column_it = std::find_if(columns.begin(), columns.end(),
                std::bind(namesEqual, std::cref(column_name), std::placeholders::_1))))
            {
                removed = true;
                column_it = columns.erase(column_it);
                columns_description.defaults.erase(column_name);
            }

            return removed;
        };

        if (!remove_column(columns_description.ordinary) &&
            !remove_column(columns_description.materialized) &&
            !remove_column(columns_description.aliases))
        {
            throw Exception("Wrong column name. Cannot find column " + column_name + " to drop",
                            ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (type == MODIFY_COLUMN)
    {
        if (codec)
            columns_description.codecs[column_name] = codec;

        if (!is_mutable())
        {
            auto & comments = columns_description.comments;
            if (comment.empty())
            {
                if (auto it = comments.find(column_name); it != comments.end())
                    comments.erase(it);
            }
            else
                columns_description.comments[column_name] = comment;

            return;
        }

        const auto default_it = columns_description.defaults.find(column_name);
        const auto had_default_expr = default_it != std::end(columns_description.defaults);
        const auto old_default_kind = had_default_expr ? default_it->second.kind : ColumnDefaultKind{};

        /// target column list
        auto & new_columns =
            default_kind == ColumnDefaultKind::Default ? columns_description.ordinary
            : default_kind == ColumnDefaultKind::Materialized ? columns_description.materialized
            : columns_description.aliases;

        /// find column or throw exception
        const auto find_column = [this] (NamesAndTypesList & columns)
        {
            const auto it = std::find_if(columns.begin(), columns.end(),
                std::bind(namesEqual, std::cref(column_name), std::placeholders::_1));
            if (it == columns.end())
                throw Exception("Wrong column name. Cannot find column " + column_name + " to modify",
                                ErrorCodes::ILLEGAL_COLUMN);

            return it;
        };

        /// if default types differ, remove column from the old list, then add to the new list
        if (default_kind != old_default_kind)
        {
            /// source column list
            auto & old_columns =
                old_default_kind == ColumnDefaultKind::Default ? columns_description.ordinary
                : old_default_kind == ColumnDefaultKind::Materialized ? columns_description.materialized
                : columns_description.aliases;

            const auto old_column_it = find_column(old_columns);
            new_columns.emplace_back(*old_column_it);
            old_columns.erase(old_column_it);

            /// do not forget to change the default type of old column
            if (had_default_expr)
                columns_description.defaults[column_name].kind = default_kind;
        }

        /// find column in one of three column lists
        const auto column_it = find_column(new_columns);
        column_it->type = data_type;

        if (!default_expression && had_default_expr)
            /// new column has no default expression, remove it from column_defaults along with it's type
            columns_description.defaults.erase(column_name);
        else if (default_expression && !had_default_expr)
            /// new column has a default expression while the old one had not, add it it column_defaults
            columns_description.defaults.emplace(column_name, ColumnDefault{default_kind, default_expression});
        else if (had_default_expr)
            /// both old and new columns have default expression, update it
            columns_description.defaults[column_name].expression = default_expression;
    }
    else if (type == MODIFY_ORDER_BY)
    {
        if (!primary_key_ast)
        {
            /// Primary and sorting key become independent after this ALTER so we have to
            /// save the old ORDER BY expression as the new primary key.
            primary_key_ast = order_by_ast->clone();
        }

        order_by_ast = order_by;
    }
    else if (type == COMMENT_COLUMN)
    {
        columns_description.comments[column_name] = comment;
    }
    else if (type == ADD_INDEX)
    {
        if (std::any_of(
                indices_description.indices.cbegin(),
                indices_description.indices.cend(),
                [this](const ASTPtr & index_ast)
                {
                    return typeid_cast<const ASTIndexDeclaration &>(*index_ast).name == index_name;
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
                        return typeid_cast<const ASTIndexDeclaration &>(*index_ast).name == after_index_name;
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
                    return typeid_cast<const ASTIndexDeclaration &>(*index_ast).name == index_name;
                });

        if (erase_it == indices_description.indices.end())
            throw Exception("Wrong index name. Cannot find index `" + index_name + "` to drop.",
                    ErrorCodes::LOGICAL_ERROR);

        indices_description.indices.erase(erase_it);
    }
    else
        throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
}

bool AlterCommand::is_mutable() const
{
    if (type == COMMENT_COLUMN)
        return false;
    if (type == MODIFY_COLUMN)
        return data_type.get() || default_expression;
    // TODO: возможно, здесь нужно дополнить
    return true;
}

void AlterCommands::apply(ColumnsDescription & columns_description, IndicesDescription & indices_description,
        ASTPtr & order_by_ast, ASTPtr & primary_key_ast) const
{
    auto new_columns_description = columns_description;
    auto new_indices_description = indices_description;
    auto new_order_by_ast = order_by_ast;
    auto new_primary_key_ast = primary_key_ast;

    for (const AlterCommand & command : *this)
        if (!command.ignore)
            command.apply(new_columns_description, new_indices_description, new_order_by_ast, new_primary_key_ast);
    columns_description = std::move(new_columns_description);
    indices_description = std::move(new_indices_description);
    order_by_ast = std::move(new_order_by_ast);
    primary_key_ast = std::move(new_primary_key_ast);
}

void AlterCommands::validate(const IStorage & table, const Context & context)
{
    auto all_columns = table.getColumns().getAll();
    auto defaults = table.getColumns().defaults;

    std::vector<std::pair<NameAndTypePair, AlterCommand *>> defaulted_columns{};

    auto default_expr_list = std::make_shared<ASTExpressionList>();
    default_expr_list->children.reserve(defaults.size());

    for (AlterCommand & command : *this)
    {
        if (command.type == AlterCommand::ADD_COLUMN || command.type == AlterCommand::MODIFY_COLUMN)
        {
            const auto & column_name = command.column_name;
            const auto column_it = std::find_if(std::begin(all_columns), std::end(all_columns),
                std::bind(namesEqual, std::cref(command.column_name), std::placeholders::_1));

            if (command.type == AlterCommand::ADD_COLUMN)
            {
                if (std::end(all_columns) != column_it)
                {
                    if (command.if_not_exists)
                        command.ignore = true;
                    else
                        throw Exception{"Cannot add column " + column_name + ": column with this name already exists", ErrorCodes::ILLEGAL_COLUMN};
                }
            }
            else if (command.type == AlterCommand::MODIFY_COLUMN)
            {

                if (std::end(all_columns) == column_it)
                {
                    if (command.if_exists)
                        command.ignore = true;
                    else
                        throw Exception{"Wrong column name. Cannot find column " + column_name + " to modify", ErrorCodes::ILLEGAL_COLUMN};
                }

                if (!command.ignore)
                {
                    all_columns.erase(column_it);
                    defaults.erase(column_name);
                }
            }

            if (!command.ignore)
            {
                /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
                all_columns.emplace_back(column_name, command.data_type ? command.data_type : std::make_shared<DataTypeUInt8>());

                if (command.default_expression)
                {
                    if (command.data_type)
                    {
                        const auto &final_column_name = column_name;
                        const auto tmp_column_name = final_column_name + "_tmp";
                        const auto column_type_raw_ptr = command.data_type.get();

                        default_expr_list->children.emplace_back(setAlias(
                            makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                                std::make_shared<ASTLiteral>(column_type_raw_ptr->getName())),
                            final_column_name));

                        default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), tmp_column_name));

                        defaulted_columns.emplace_back(NameAndTypePair{column_name, command.data_type}, &command);
                    }
                    else
                    {
                        /// no type explicitly specified, will deduce later
                        default_expr_list->children.emplace_back(
                            setAlias(command.default_expression->clone(), column_name));

                        defaulted_columns.emplace_back(NameAndTypePair{column_name, nullptr}, &command);
                    }
                }
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            for (const auto & default_column : defaults)
            {
                const auto & default_expression = default_column.second.expression;
                ASTPtr query = default_expression;
                auto syntax_result = SyntaxAnalyzer(context).analyze(query, all_columns);
                const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
                const auto required_columns = actions->getRequiredColumns();

                if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                    throw Exception(
                        "Cannot drop column " + command.column_name + ", because column " + default_column.first +
                        " depends on it", ErrorCodes::ILLEGAL_COLUMN);
            }

            auto found = false;
            for (auto it = std::begin(all_columns); it != std::end(all_columns);)
            {
                if (namesEqual(command.column_name, *it))
                {
                    found = true;
                    it = all_columns.erase(it);
                }
                else
                    ++it;
            }

            for (auto it = std::begin(defaults); it != std::end(defaults);)
            {
                if (namesEqual(command.column_name, { it->first, nullptr }))
                    it = defaults.erase(it);
                else
                    ++it;
            }

            if (!found)
            {
                if (command.if_exists)
                    command.ignore = true;
                else
                    throw Exception("Wrong column name. Cannot find column " + command.column_name + " to drop",
                        ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else if (command.type == AlterCommand::COMMENT_COLUMN)
        {
            const auto column_it = std::find_if(std::begin(all_columns), std::end(all_columns),
                                                std::bind(namesEqual, std::cref(command.column_name), std::placeholders::_1));
            if (column_it == std::end(all_columns))
            {
                if (command.if_exists)
                    command.ignore = true;
                else
                    throw Exception{"Wrong column name. Cannot find column " + command.column_name + " to comment", ErrorCodes::ILLEGAL_COLUMN};
            }
        }
    }

    /** Existing defaulted columns may require default expression extensions with a type conversion,
        *  therefore we add them to defaulted_columns to allow further processing */
    for (const auto & col_def : defaults)
    {
        const auto & column_name = col_def.first;
        const auto column_it = std::find_if(all_columns.begin(), all_columns.end(), [&] (const NameAndTypePair & name_type)
            { return namesEqual(column_name, name_type); });

        const auto tmp_column_name = column_name + "_tmp";
        const auto & column_type_ptr = column_it->type;

            default_expr_list->children.emplace_back(setAlias(
                makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                    std::make_shared<ASTLiteral>(column_type_ptr->getName())),
                column_name));

        default_expr_list->children.emplace_back(setAlias(col_def.second.expression->clone(), tmp_column_name));

        defaulted_columns.emplace_back(NameAndTypePair{column_name, column_type_ptr}, nullptr);
    }

    ASTPtr query = default_expr_list;
    auto syntax_result = SyntaxAnalyzer(context).analyze(query, all_columns);
    const auto actions = ExpressionAnalyzer(query, syntax_result, context).getActions(true);
    const auto block = actions->getSampleBlock();

    /// set deduced types, modify default expression if necessary
    for (auto & defaulted_column : defaulted_columns)
    {
        const auto & name_and_type = defaulted_column.first;
        AlterCommand * & command_ptr = defaulted_column.second;

        const auto & column_name = name_and_type.name;
        const auto has_explicit_type = nullptr != name_and_type.type;

        /// default expression on old column
        if (has_explicit_type)
        {
            const auto & explicit_type = name_and_type.type;
            const auto & tmp_column = block.getByName(column_name + "_tmp");
            const auto & deduced_type = tmp_column.type;

            // column not specified explicitly in the ALTER query may require default_expression modification
            if (!explicit_type->equals(*deduced_type))
            {
                const auto default_it = defaults.find(column_name);

                /// column has no associated alter command, let's create it
                if (!command_ptr)
                {
                    /// add a new alter command to modify existing column
                    this->emplace_back(AlterCommand{
                        AlterCommand::MODIFY_COLUMN, column_name, explicit_type,
                        default_it->second.kind, default_it->second.expression
                    });

                    command_ptr = &this->back();
                }

                command_ptr->default_expression = makeASTFunction("CAST", command_ptr->default_expression->clone(),
                    std::make_shared<ASTLiteral>(explicit_type->getName()));
            }
        }
        else
        {
            /// just set deduced type
            command_ptr->data_type = block.getByName(column_name).type;
        }
    }
}

void AlterCommands::apply(ColumnsDescription & columns_description) const
{
    auto out_columns_description = columns_description;
    IndicesDescription indices_description;
    ASTPtr out_order_by;
    ASTPtr out_primary_key;
    apply(out_columns_description, indices_description, out_order_by, out_primary_key);

    if (out_order_by)
        throw Exception("Storage doesn't support modifying ORDER BY expression", ErrorCodes::NOT_IMPLEMENTED);
    if (out_primary_key)
        throw Exception("Storage doesn't support modifying PRIMARY KEY expression", ErrorCodes::NOT_IMPLEMENTED);
    if (!indices_description.indices.empty())
        throw Exception("Storage doesn't support modifying indices", ErrorCodes::NOT_IMPLEMENTED);

    columns_description = std::move(out_columns_description);
}

bool AlterCommands::is_mutable() const
{
    for (const auto & param : *this)
    {
        if (param.is_mutable())
            return true;
    }

    return false;
}

}
