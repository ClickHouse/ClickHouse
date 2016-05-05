#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/ColumnDefault.h>

namespace DB
{

/// Для RESHARD PARTITION.
using WeightedZooKeeperPath = std::pair<String, UInt64>;
using WeightedZooKeeperPaths = std::vector<WeightedZooKeeperPath>;

/// Операция из запроса ALTER (кроме манипуляции с PART/PARTITION). Добавление столбцов типа Nested не развернуто в добавление отдельных столбцов.
struct AlterCommand
{
	enum Type
	{
		ADD_COLUMN,
		DROP_COLUMN,
		MODIFY_COLUMN,
		MODIFY_PRIMARY_KEY,
	};

	Type type;

	String column_name;

	/// Для ADD и MODIFY - новый тип столбца.
	DataTypePtr data_type;

	ColumnDefaultType default_type{};
	ASTPtr default_expression{};

	/// Для ADD - после какого столбца добавить новый. Если пустая строка, добавить в конец. Добавить в начало сейчас нельзя.
	String after_column;

	/// Для MODIFY_PRIMARY_KEY
	ASTPtr primary_key;

	/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
	static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
	{
		String name_with_dot = name_without_dot + ".";
		return (name_with_dot == name_type.name.substr(0, name_without_dot.length() + 1) || name_without_dot == name_type.name);
	}

	void apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const;

	AlterCommand() = default;
	AlterCommand(const Type type, const String & column_name, const DataTypePtr & data_type,
				 const ColumnDefaultType default_type, const ASTPtr & default_expression,
				 const String & after_column = String{})
		: type{type}, column_name{column_name}, data_type{data_type}, default_type{default_type},
		default_expression{default_expression}, after_column{after_column}
	{}
};

class IStorage;
class Context;

class AlterCommands : public std::vector<AlterCommand>
{
public:
	void apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const;

	void validate(IStorage * table, const Context & context);
};

}
