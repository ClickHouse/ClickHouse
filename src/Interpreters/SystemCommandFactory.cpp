#include <Interpreters/SystemCommandFactory.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

SystemCommandFactory & SystemCommandFactory::instance()
{
    static SystemCommandFactory commands_fact;
    return commands_fact;
}

SystemCommandFactory::ExecuteFn
SystemCommandFactory::get(InterpreterSystemQuery & interpreter, ContextMutablePtr system_context, ASTSystemQuery::Type type)
{
    if (commands[*magic_enum::enum_index(type)])
    {
        Arguments arguments{.interpreter = interpreter, .system_context = system_context};
        return commands[*magic_enum::enum_index(type)](arguments);
    }
    else
    {
        return {};
    }
}

void SystemCommandFactory::registerCommand(ASTSystemQuery::Type type, CreatorFn creator_fn)
{
    if (commands[*magic_enum::enum_index(type)])
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "SystemCommandFactory: the system command '{}' already registered",
            ASTSystemQuery::typeToString(type));
    }

    commands[*magic_enum::enum_index(type)] = creator_fn;
}
}
