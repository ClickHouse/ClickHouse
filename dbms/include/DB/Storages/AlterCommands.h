#pragma once
#include <DB/Core/NamesAndTypes.h>
#include <DB/DataTypes/DataTypeNested.h>

namespace DB
{

/// Операция из запроса ALTER. Добавление столбцов типа Nested не развернуто в добавление отдельных столбцов.
struct AlterCommand
{
	enum Type
	{
		ADD,
		DROP,
		MODIFY
	};

	Type type;

	String column_name;

	/// Для ADD и MODIFY - новый тип столбца.
	DataTypePtr data_type;

	/// Для ADD - после какого столбца добавить новый. Если пустая строка, добавить в конец. Добавить в начало сейчас нельзя.
	String after_column;


	/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
	static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
	{
		String name_with_dot = name_without_dot + ".";
		return (name_with_dot == name_type.name.substr(0, name_without_dot.length() + 1) || name_without_dot == name_type.name);
	}

	void apply(NamesAndTypesList & columns)
	{
		if (type == ADD)
		{
			NamesAndTypesList::iterator insert_it = columns.end();
			if (!after_column.empty())
			{
				/// Пытаемся найти первую с конца колонку с именем column_name или с именем, начинающимся с column_name и ".".
				/// Например "fruits.bananas"
				/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
				NamesAndTypesList::reverse_iterator reverse_insert_it = std::find_if(columns.rbegin(), columns.rend(),
					std::bind(namesEqual, after_column, _1));

				if (reverse_insert_it == columns.rend())
					throw Exception("Wrong column name. Cannot find column " + column_name + " to insert after",
									DB::ErrorCodes::ILLEGAL_COLUMN);
				else
				{
					/// base возвращает итератор, уже смещенный на один элемент вправо
					insert_it = reverse_insert_it.base();
				}
			}

			columns.insert(insert_it, NameAndTypePair(column_name, data_type));

			/// Медленно, так как каждый раз копируется список
			columns = *DataTypeNested::expandNestedColumns(columns);
		}
		else if (type == DROP)
		{
			bool is_first = true;
			NamesAndTypesList::iterator column_it;
			do
			{
				column_it = std::find_if(columns.begin(), columns.end(), std::bind(namesEqual, column_name, _1));

				if (column_it == columns.end())
				{
					if (is_first)
						throw Exception("Wrong column name. Cannot find column " + column_name + " to drop",
										DB::ErrorCodes::ILLEGAL_COLUMN);
				}
				else
					columns.erase(column_it);
				is_first = false;
			}
			while (column_it != columns.end());
		}
		else if (type == MODIFY)
		{
			NamesAndTypesList::iterator column_it = std::find_if(columns->begin(), columns->end(),
				std::bind(namesEqual, column_name, _1) );
			if (column_it == columns->end())
				throw Exception("Wrong column name. Cannot find column " + column_name + " to modify.",
								DB::ErrorCodes::ILLEGAL_COLUMN);
			column_it->type = data_type;
		}
		else
			throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
	}
};

class AlterCommands : public std::vector<AlterCommand>
{
public:
	void apply(NamesAndTypesList & columns)
	{
		NamesAndTypesList new_columns;
		for (const AlterCommand & command : *this)
			command.apply(new_columns);
		columns = new_columns;
	}
};

}
