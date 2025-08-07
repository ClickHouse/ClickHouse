#pragma once

#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterFinishCollectingWorkload : public IInterpreter
{
public:
    InterpreterFinishCollectingWorkload(const ASTPtr &, ContextMutablePtr context_)
        : context(context_)
    {
    }

    BlockIO execute() override;

protected:
    ContextMutablePtr context;
};

}
