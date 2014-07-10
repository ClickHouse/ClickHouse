#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>

namespace DB
{
class ASTIdentifier;

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

private:
	void dropColumnFromAST(const ASTIdentifier & drop_column, ASTs & columns);
	void addColumnToAST(StoragePtr table, ASTs & columns, const ASTPtr & add_column_ptr, const ASTPtr & after_column_ptr);

	ASTPtr query_ptr;
	
	Context context;
};
}
