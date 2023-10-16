#include <Interpreters/Streaming/EmitInterpreter.h>
#include <Common/IntervalKind.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Streaming
{

void EmitInterpreter::checkEmitAST(ASTPtr & query)
{
    auto select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        return;

    auto emit_query = select_query->emit();
    if (!emit_query)
        return;

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    if (emit->periodic_interval)
        checkIntervalAST(emit->periodic_interval, "Invalid EMIT PERIODIC interval");
}
}
}
