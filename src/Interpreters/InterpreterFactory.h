#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;

class InterpreterFactory
{
public:
    static std::unique_ptr<IInterpreter> get(
        ASTPtr & query,
        Context & context,
        const SelectQueryOptions & options = {});
};

}
