#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{


/** Переименовать одну или несколько таблиц.
  */
class InterpreterRenameQuery : public IInterpreter
{
public:
	InterpreterRenameQuery(ASTPtr query_ptr_, Context & context_);
	BlockIO execute() override;

private:
	ASTPtr query_ptr;
	Context context;
};


}
