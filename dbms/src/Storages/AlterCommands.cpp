#include <DB/Storages/AlterCommands.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>

namespace DB
{
	void AlterCommand::apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const
	{
		if (type == ADD)
		{
			const auto throw_if_exists = [this] (const NamesAndTypesList & columns) {
				if (std::count_if(columns.begin(), columns.end(),
					std::bind(namesEqual, std::cref(column_name), std::placeholders::_1)))
				{
					throw Exception("Cannot add column " + column_name + ": column with this name already exisits.",
					DB::ErrorCodes::ILLEGAL_COLUMN);
				}
			};

			throw_if_exists(columns);
			throw_if_exists(materialized_columns);
			throw_if_exists(alias_columns);

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

			if (old_default_type != default_type)
				throw Exception{"Cannot change column default specifier from " + toString(old_default_type) +
					" to " + toString(default_type), 0 };

			/// find column or throw exception
			const auto find_column = [this] (NamesAndTypesList & columns) {
				const auto it = std::find_if(columns.begin(), columns.end(),
					std::bind(namesEqual, std::cref(column_name), std::placeholders::_1) );
				if (it == columns.end())
					throw Exception("Wrong column name. Cannot find column " + column_name + " to modify.",
									DB::ErrorCodes::ILLEGAL_COLUMN);

				return it;
			};

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
}
