#pragma once
#include <Access/Common/AccessType.h>
#include <Interpreters/InterpreterSystemQuery.h>
#include <Parsers/IAST.h>
#include <QueryPipeline/BlockIO.h>

namespace DB
{
class ISystemCommand
{
public:
    explicit ISystemCommand(InterpreterSystemQuery & interpreter_)
        : interpreter(interpreter_)
        , log(interpreter_.log)
        , query(interpreter_.query_ptr->as<ASTSystemQuery &>())
    {
    }

    virtual ~ISystemCommand() = default;

    BlockIO execute();

    virtual std::optional<AccessType> getAccess() = 0;
    virtual AccessRightsElements getRequiredAccessForDDLOnCluster() = 0;
    virtual void executeImpl(BlockIO &) = 0;

protected:
    ContextMutablePtr getContext();
    InterpreterSystemQuery & interpreter;
    LoggerPtr log;
    ASTSystemQuery & query;
};

}
