#include <Interpreters/Streaming/EmitInterpreter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Common/IntervalKind.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Streaming
{
namespace
{
/// proton: porting note. TODO: remove this function. Using the function defined in WindowCommon.h
std::optional<IntervalKind> mapIntervalKind(const String & func_name)
{
    if (func_name == "toIntervalNanosecond")
        return IntervalKind::Nanosecond;
    else if (func_name == "toIntervalMicrosecond")
        return IntervalKind::Microsecond;
    else if (func_name == "toIntervalMillisecond")
        return IntervalKind::Millisecond;
    else if (func_name == "toIntervalSecond")
        return IntervalKind::Second;
    else if (func_name == "toIntervalMinute")
        return IntervalKind::Minute;
    else if (func_name == "toIntervalHour")
        return IntervalKind::Hour;
    else if (func_name == "toIntervalDay")
        return IntervalKind::Day;
    else if (func_name == "toIntervalWeek")
        return IntervalKind::Week;
    else if (func_name == "toIntervalMonth")
        return IntervalKind::Month;
    else if (func_name == "toIntervalQuarter")
        return IntervalKind::Quarter;
    else if (func_name == "toIntervalYear")
        return IntervalKind::Year;
    else
        return {};
}

void checkIntervalAST(const ASTPtr & ast, const String & msg)
{
    assert(ast);
    auto func_node = ast->as<ASTFunction>();
    if (func_node)
    {
        auto kind = mapIntervalKind(func_node->name);
        if (kind)
        {
            if (*kind <= IntervalKind::Day)
                return;
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}: the max interval kind supported is DAY.", msg);
        }
    }
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}", msg);
}
}

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
