#include <DB/Storages/AlterCommands.h>
#include <DB/Storages/IStorage.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Parsers/ASTIdentifier.h>

namespace DB
{
	void AlterCommand::apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const
	{
		if (type == ADD)
		{
			const auto exists_in = [this] (const NamesAndTypesList & columns) {
				return columns.end() != std::find_if(columns.begin(), columns.end(),
					std::bind(namesEqual, std::cref(column_name), std::placeholders::_1));
			};

			if (exists_in(columns) ||
				exists_in(materialized_columns) ||
				exists_in(alias_columns))
			{
				throw Exception{
					"Cannot add column " + column_name + ": column with this name already exisits.",
					DB::ErrorCodes::ILLEGAL_COLUMN
				};
			}

			const auto add_column = [this] (NamesAndTypesList & columns) {
				auto insert_it = columns.end();

				if (!after_column.empty())
				{
					/// Пытаемся найти первую с конца колонку с именем column_name или с именем, начинающимся с column_name и ".".
					/// Например "fruits.bananas"
					/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
					const auto reverse_insert_it = std::find_if(columns.rbegin(), columns.rend(),
						std::bind(namesEqual, std::cref(after_column), std::placeholders::_1));

					if (reverse_insert_it == columns.rend())
						throw Exception("Wrong column name. Cannot find column " + column_name + " to insert after",
										DB::ErrorCodes::ILLEGAL_COLUMN);
					else
					{
						/// base возвращает итератор, уже смещенный на один элемент вправо
						insert_it = reverse_insert_it.base();
					}
				}

				columns.emplace(insert_it, column_name, data_type);
			};

			if (default_type == ColumnDefaultType::Default)
				add_column(columns);
			else if (default_type == ColumnDefaultType::Materialized)
				add_column(materialized_columns);
			else if (default_type == ColumnDefaultType::Alias)
				add_column(alias_columns);
			else
				throw Exception{"Unknown ColumnDefaultType value", ErrorCodes::LOGICAL_ERROR};

			if (default_expression)
				column_defaults.emplace(column_name, ColumnDefault{default_type, default_expression});

			/// Медленно, так как каждый раз копируется список
			columns = *DataTypeNested::expandNestedColumns(columns);
		}
		else if (type == DROP)
		{
			/// look for a column in list and remove it if present, also removing corresponding entry from column_defaults
			const auto remove_column = [&column_defaults, this] (NamesAndTypesList & columns) {
				auto removed = false;
				NamesAndTypesList::iterator column_it;

				while (columns.end() != (column_it = std::find_if(columns.begin(), columns.end(),
					std::bind(namesEqual, std::cref(column_name), std::placeholders::_1))))
				{
					removed = true;
					column_it = columns.erase(column_it);
					column_defaults.erase(column_name);
				}

				return removed;
			};

			if (!remove_column(columns) &&
				!remove_column(materialized_columns) &&
				!remove_column(alias_columns))
			{
				throw Exception("Wrong column name. Cannot find column " + column_name + " to drop",
								DB::ErrorCodes::ILLEGAL_COLUMN);
			}
		}
		else if (type == MODIFY)
		{
			const auto it = column_defaults.find(column_name);
			const auto had_default_expr = it != column_defaults.end();
			const auto old_default_type = had_default_expr ? it->second.type : ColumnDefaultType{};

			/// allow conversion between DEFAULT and MATERIALIZED
			const auto default_materialized_conversion =
				(old_default_type == ColumnDefaultType::Default && default_type == ColumnDefaultType::Materialized) ||
				(old_default_type == ColumnDefaultType::Materialized && default_type == ColumnDefaultType::Default);

			if (old_default_type != default_type && !default_materialized_conversion)
				throw Exception{"Cannot change column default specifier from " + toString(old_default_type) +
					" to " + toString(default_type), ErrorCodes::INCORRECT_QUERY};

			/// find column or throw exception
			const auto find_column = [this] (NamesAndTypesList & columns) {
				const auto it = std::find_if(columns.begin(), columns.end(),
					std::bind(namesEqual, std::cref(column_name), std::placeholders::_1) );
				if (it == columns.end())
					throw Exception("Wrong column name. Cannot find column " + column_name + " to modify.",
									DB::ErrorCodes::ILLEGAL_COLUMN);

				return it;
			};

			/// remove from the old list, add to the new list in case of DEFAULT <-> MATERIALIZED alteration
			if (default_materialized_conversion)
			{
				const auto was_default = old_default_type == ColumnDefaultType::Default;
				auto & old_columns = was_default ? columns : materialized_columns;
				auto & new_columns = was_default ? materialized_columns : columns;

				const auto column_it = find_column(old_columns);
				new_columns.emplace_back(*column_it);
				old_columns.erase(column_it);

				/// do not forget to change the default type of old column
				if (had_default_expr)
					column_defaults[column_name].type = default_type;
			}

			/// find column in one of three column lists
			const auto column_it = find_column(
				default_type == ColumnDefaultType::Default ? columns :
				default_type == ColumnDefaultType::Materialized ? materialized_columns :
				alias_columns);

			column_it->type = data_type;

			/// remove, add or update default_expression
			if (!default_expression && had_default_expr)
				column_defaults.erase(column_name);
			else if (default_expression && !had_default_expr)
				column_defaults.emplace(column_name, ColumnDefault{default_type, default_expression});
			else if (had_default_expr)
				column_defaults[column_name].expression = default_expression;
		}
		else
			throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
	}

	void AlterCommands::apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const
	{
		auto new_columns = columns;
		auto new_materialized_columns = materialized_columns;
		auto new_alias_columns = alias_columns;
		auto new_column_defaults = column_defaults;

		for (const AlterCommand & command : *this)
			command.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

		columns = std::move(new_columns);
		materialized_columns = std::move(new_materialized_columns);
		alias_columns = std::move(new_alias_columns);
		column_defaults = std::move(new_column_defaults);
	}

	void AlterCommands::validate(IStorage * table, const Context & context)
	{
		auto columns = table->getColumnsList();
		columns.insert(std::end(columns), std::begin(table->alias_columns), std::end(table->alias_columns));
		auto defaults = table->column_defaults;

		std::vector<std::pair<String, AlterCommand *>> defaulted_columns{};

		ASTPtr default_expr_list{new ASTExpressionList};
		default_expr_list->children.reserve(defaults.size());

		for (AlterCommand & command : *this)
		{
			if (command.type == AlterCommand::ADD || command.type == AlterCommand::MODIFY)
			{
				if (command.type == AlterCommand::MODIFY)
				{
					const auto it = std::find_if(std::begin(columns), std::end(columns),
						std::bind(AlterCommand::namesEqual, std::cref(command.column_name), std::placeholders::_1));

					if (it == std::end(columns))
						throw Exception("Wrong column name. Cannot find column " + command.column_name + " to modify.",
										DB::ErrorCodes::ILLEGAL_COLUMN);

					columns.erase(it);
					defaults.erase(command.column_name);
				}

				/// we're creating dummy DataTypeUInt8 in order to prevent the NullPointerException in ExpressionActions
				columns.emplace_back(command.column_name, command.data_type ? command.data_type : new DataTypeUInt8);

				if (command.default_expression)
				{
					if (command.data_type)
					{
						const auto & final_column_name = command.column_name;
						const auto tmp_column_name = final_column_name + "_tmp";
						const auto data_type_ptr = command.data_type.get();

						/// specific code for different data types, e.g. toFixedString(col, N) for DataTypeFixedString
						if (const auto fixed_string = typeid_cast<const DataTypeFixedString *>(data_type_ptr))
						{
							const auto conversion_function_name = "toFixedString";

							default_expr_list->children.emplace_back(setAlias(
								makeASTFunction(
									conversion_function_name,
									ASTPtr{new ASTIdentifier{{}, tmp_column_name}},
									ASTPtr{new ASTLiteral{{}, fixed_string->getN()}}),
								final_column_name));
						}
						else
						{
							/// @todo fix for parametric types, results in broken codem, i.e. toArray(ElementType)(col)
							const auto conversion_function_name = "to" + data_type_ptr->getName();

							default_expr_list->children.emplace_back(setAlias(
								makeASTFunction(conversion_function_name, ASTPtr{new ASTIdentifier{{}, tmp_column_name}}),
								final_column_name));
						}

						default_expr_list->children.emplace_back(setAlias(command.default_expression->clone(), tmp_column_name));

						defaulted_columns.emplace_back(command.column_name, &command);
					}
					else
					{
						default_expr_list->children.emplace_back(
							setAlias(command.default_expression->clone(), command.column_name));

						defaulted_columns.emplace_back(command.column_name, &command);
					}
				}
			}
			else if (command.type == AlterCommand::DROP)
			{
				auto found = false;
				for (auto it = std::begin(columns); it != std::end(columns);)
					if (AlterCommand::namesEqual(command.column_name, *it))
					{
						found = true;
						it = columns.erase(it);
					}
					else
						++it;

				for (auto it = std::begin(defaults); it != std::end(defaults);)
					if (AlterCommand::namesEqual(command.column_name, { it->first, nullptr }))
						it = defaults.erase(it);
					else
						++it;

				if (!found)
					throw Exception("Wrong column name. Cannot find column " + command.column_name + " to drop.",
						DB::ErrorCodes::ILLEGAL_COLUMN);
			}
		}

		/** Existing defaulted columns may require default expression extensions with a type conversion,
		 *  therefore we add them to defaulted_columns to allow further processing */
		for (const auto & col_def : defaults)
		{
			const auto & column_name = col_def.first;
			const auto column_it = std::find_if(columns.begin(), columns.end(), [&] (const NameAndTypePair & name_type) {
				return AlterCommand::namesEqual(column_name, name_type);
			});
			const auto tmp_column_name = column_name + "_tmp";
			const auto conversion_function_name = "to" + column_it->type->getName();

			default_expr_list->children.emplace_back(setAlias(
				makeASTFunction(conversion_function_name, ASTPtr{new ASTIdentifier{{}, tmp_column_name}}),
				column_name));

			default_expr_list->children.emplace_back(setAlias(col_def.second.expression->clone(), tmp_column_name));

			defaulted_columns.emplace_back(column_name, nullptr);
		}

		const auto actions = ExpressionAnalyzer{default_expr_list, context, columns}.getActions(true);
		const auto block = actions->getSampleBlock();

		/// set deduced types, modify default expression if necessary
		for (auto & defaulted_column : defaulted_columns)
		{
			const auto & column_name = defaulted_column.first;
			const auto command_ptr = defaulted_column.second;
			const auto & column = block.getByName(column_name);

			/// default expression on old column
			if (!command_ptr)
			{
				const auto & tmp_column = block.getByName(column_name + "_tmp");

				// column not specified explicitly in the ALTER query may require default_expression modification
				if (column.type->getName() != tmp_column.type->getName())
				{
					const auto it = defaults.find(column_name);
					this->push_back(AlterCommand{
						AlterCommand::MODIFY, column_name, column.type, it->second.type,
						makeASTFunction("to" + column.type->getName(), it->second.expression),
					});
				}
			}
			else if (command_ptr && command_ptr->data_type)
			{
				const auto & tmp_column = block.getByName(column_name + "_tmp");

				/// type mismatch between explicitly specified and deduced type, add conversion
				if (column.type->getName() != tmp_column.type->getName())
				{
					command_ptr->default_expression = makeASTFunction(
						"to" + column.type->getName(),
						command_ptr->default_expression->clone());
				}
			}
			else
			{
				/// just set deduced type
				command_ptr->data_type = column.type;
			}
		}
	}
}
