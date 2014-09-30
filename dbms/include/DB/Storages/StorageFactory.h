#pragma once

#include <DB/Storages/IStorage.h>


namespace DB
{

class Context;


/** Позволяет создать таблицу по имени движка.
  */
class StorageFactory
{
public:
	StoragePtr get(
		const String & name,
		const String & data_path,
		const String & table_name,
		const String & database_name,
		Context & local_context,
		Context & context,
		ASTPtr & query,
		NamesAndTypesListPtr columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults,
		bool attach) const;
};

}
