#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Переименовать одну или несколько таблиц.
  */
class InterpreterRenameQuery
{
public:
	InterpreterRenameQuery(ASTPtr query_ptr_, Context & context_);
	void execute();

private:
	ASTPtr query_ptr;
	Context context;
};


}
