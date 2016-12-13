#pragma once

#include <DB/Interpreters/IInterpreter.h>


namespace DB
{

class Context;

class InterpreterFactory
{
public:
	static std::unique_ptr<IInterpreter> get(
		ASTPtr & query,
		Context & context,
		QueryProcessingStage::Enum stage = QueryProcessingStage::Complete);
};

}
