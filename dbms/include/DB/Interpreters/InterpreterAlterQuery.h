#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>

namespace DB
{

/** Позволяет добавить или удалить столбец в таблицу
  */
class InterpreterAlterQuery
{
public:
	InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_);

	void execute();

private:
	ASTPtr query_ptr;
	
	Context context;
};
}
