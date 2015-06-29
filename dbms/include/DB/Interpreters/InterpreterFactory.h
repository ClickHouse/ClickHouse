#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterFactory
{
public:
	static SharedPtr<IInterpreter> get(
		ASTPtr & query,
		Context & context,
		QueryProcessingStage::Enum stage = QueryProcessingStage::Complete);
};

}
