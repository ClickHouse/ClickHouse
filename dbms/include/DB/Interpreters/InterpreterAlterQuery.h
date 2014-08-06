#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Storages/AlterCommands.h>
#include <DB/Interpreters/Context.h>
#include <DB/Parsers/ASTIdentifier.h>

namespace DB
{



/** Позволяет добавить или удалить столбец в таблице.
  */
class InterpreterAlterQuery
{
public:
	InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_);

	void execute();

	/** Изменяет список столбцов в метаданных таблицы на диске. Нужно вызывать под TableStructureLock соответствующей таблицы.
	  */
	static void updateMetadata(const String & database, const String & table, const NamesAndTypesList & columns, Context & context);
private:
	typedef std::vector<Field> Partitions;

	ASTPtr query_ptr;
	
	Context context;

	static void parseAlter(const ASTAlterQuery::ParameterContainer & params, const DataTypeFactory & data_type_factory,
		AlterCommands & out_commands, Partitions & out_partitions_to_drop);
};
}
