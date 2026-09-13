#pragma once
#include <functional>

#include <Interpreters/InterpreterSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <QueryPipeline/BlockIO.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class SystemCommandFactory : private boost::noncopyable
{
public:
    static SystemCommandFactory & instance();

    struct Arguments
    {
        InterpreterSystemQuery & interpreter;
        ContextMutablePtr system_context;

    };

    using ExecuteFn = std::function<BlockIO()>;
    using CreatorFn = std::function<ExecuteFn(Arguments & arguments)>;

    ExecuteFn get(InterpreterSystemQuery & interpreter, ContextMutablePtr system_context, ASTSystemQuery::Type type);

    void registerCommand(ASTSystemQuery::Type type, CreatorFn execute_fn);

private:
    std::array<CreatorFn, magic_enum::enum_count<ASTSystemQuery::Type>()> commands;
};
}
