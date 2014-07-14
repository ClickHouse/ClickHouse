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

	void execute(bool only_metadata = false);

	/** Делает то же, что execute, но не вызывает никакие методы IStorage - только обновляет метаданные на диске.
	  * Предполагается для вызова из самого IStorage, решившего изменить набор столбцов по собственной инициативе (ReplicatedMergeTree).
	  */
	static void updateMetadata(const String & database, const String & table, const NamesAndTypesList & columns, Context & context);

	/** Изменяет список столбцов в метаданных таблицы на диске. Нужно вызывать под TableStructureLock соответствующей таблицы.
	  */
	static void updateMetadata(const String & database, const String & table, const NamesAndTypesList & columns, Context & context);

	static AlterCommands parseAlter(const ASTAlterQuery::ParameterContainer & params, const DataTypeFactory & data_type_factory);
private:
	ASTPtr query_ptr;
	
	Context context;
};
}
