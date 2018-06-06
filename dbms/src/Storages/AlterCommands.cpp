#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}


void AlterCommand::apply(ColumnsDescription & columns_description) const
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
                std::bind(namesEqual, std::cref(column_name), std::placeholders::_1) );
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
    else if (type == MODIFY_PRIMARY_KEY)
    {
        /// This have no relation to changing the list of columns.
        /// TODO Check that all columns exist, that only columns with constant defaults are added.
    }
    else
        throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
}


void AlterCommands::apply(ColumnsDescription & columns_description) const
{
    auto new_columns_description = columns_description;

    for (const AlterCommand & command : *this)
        command.apply(new_columns_description);

    columns_description = std::move(new_columns_description);
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
                std::bind(AlterCommand::namesEqual, std::cref(command.column_name), std::placeholders::_1));

            if (command.type == AlterCommand::ADD_COLUMN)
            {
                if (std::end(all_columns) != column_it)
                    throw Exception{"Cannot add column " + column_name + ": column with this name already exists", ErrorCodes::ILLEGAL_COLUMN};
            }
            else if (command.type == AlterCommand::MODIFY_COLUMN)
            {

                if (std::end(all_columns) == column_it)
                    throw Exception{"Wrong column name. Cannot find column " + column_name + " to modify", ErrorCodes::ILLEGAL_COLUMN};

                all_columns.erase(column_it);
                defaults.erase(column_name);
            }

            /// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
            all_columns.emplace_back(column_name, command.data_type ? command.data_type : std::make_shared<DataTypeUInt8>());

            if (command.default_expression)
            {
                if (command.data_type)
                {
                    const auto & final_column_name = column_name;
                    const auto tmp_column_name = final_column_name + "_tmp";
                    const auto column_type_raw_ptr = command.data_type.get();

                    default_expr_list->children.emplace_back(setAlias(
                        makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                            std::make_shared<ASTLiteral>(Field(column_type_raw_ptr->getName()))),
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
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            for (const auto & default_column : defaults)
            {
                const auto & default_expression = default_column.second.expression;
                const auto actions = ExpressionAnalyzer{default_expression, context, {}, all_columns}.getActions(true);
                const auto required_columns = actions->getRequiredColumns();

                if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), command.column_name))
                    throw Exception(
                        "Cannot drop column " + command.column_name + ", because column " + default_column.first +
                        " depends on it", ErrorCodes::ILLEGAL_COLUMN);
            }

            auto found = false;
            for (auto it = std::begin(all_columns); it != std::end(all_columns);)
            {
                if (AlterCommand::namesEqual(command.column_name, *it))
                {
                    found = true;
                    it = all_columns.erase(it);
                }
                else
                    ++it;
            }

            for (auto it = std::begin(defaults); it != std::end(defaults);)
            {
                if (AlterCommand::namesEqual(command.column_name, { it->first, nullptr }))
                    it = defaults.erase(it);
                else
                    ++it;
            }

            if (!found)
                throw Exception("Wrong column name. Cannot find column " + command.column_name + " to drop",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    /** Existing defaulted columns may require default expression extensions with a type conversion,
        *  therefore we add them to defaulted_columns to allow further processing */
    for (const auto & col_def : defaults)
    {
        const auto & column_name = col_def.first;
        const auto column_it = std::find_if(all_columns.begin(), all_columns.end(), [&] (const NameAndTypePair & name_type)
            { return AlterCommand::namesEqual(column_name, name_type); });

        const auto tmp_column_name = column_name + "_tmp";
        const auto & column_type_ptr = column_it->type;

            default_expr_list->children.emplace_back(setAlias(
                makeASTFunction("CAST", std::make_shared<ASTIdentifier>(tmp_column_name),
                    std::make_shared<ASTLiteral>(Field(column_type_ptr->getName()))),
                column_name));

        default_expr_list->children.emplace_back(setAlias(col_def.second.expression->clone(), tmp_column_name));

        defaulted_columns.emplace_back(NameAndTypePair{column_name, column_type_ptr}, nullptr);
    }

    const auto actions = ExpressionAnalyzer{default_expr_list, context, {}, all_columns}.getActions(true);
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
                    std::make_shared<ASTLiteral>(Field(explicit_type->getName())));
            }
        }
        else
        {
            /// just set deduced type
            command_ptr->data_type = block.getByName(column_name).type;
        }
    }
}

}
