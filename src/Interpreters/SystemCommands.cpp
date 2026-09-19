#include <csignal>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/SystemCommandFactory.h>
#include <Interpreters/SystemCommands.h>
#include <Common/ErrnoException.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>

namespace DB
{
ContextMutablePtr ISystemCommand::getContext()
{
    return interpreter.getContext();
}

BlockIO ISystemCommand::execute()
{
    BlockIO result;

    auto access = getAccess();
    if (access.has_value())
    {
        getContext()->checkAccess(*access);
    }

    executeImpl(result);

    return result;
}
namespace ErrorCodes
{
extern const int CANNOT_KILL;
}

class ShutdownCommand final : public ISystemCommand
{
public:
    explicit ShutdownCommand(InterpreterSystemQuery & interpreter_)
        : ISystemCommand(interpreter_)
    {
    }

    std::optional<AccessType> getAccess() override { return AccessType::SYSTEM_SHUTDOWN; }
    AccessRightsElements getRequiredAccessForDDLOnCluster() override
    {
        AccessRightsElements required_access;
        required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
        return required_access;
    }
    void executeImpl(BlockIO &) override
    {
        if (kill(0, SIGTERM))
            throw ErrnoException(ErrorCodes::CANNOT_KILL, "System call kill(0, SIGTERM) failed");
    }

    static const ASTSystemQuery::Type type = ASTSystemQuery::Type::SHUTDOWN;
    static std::shared_ptr<ISystemCommand> create(SystemCommandFactory::Arguments & args)
    {
        return std::make_shared<ShutdownCommand>(args.interpreter);
    }
};

class KillCommand final : public ISystemCommand
{
public:
    explicit KillCommand(InterpreterSystemQuery & interpreter_)
        : ISystemCommand(interpreter_)
    {
    }

    std::optional<AccessType> getAccess() override { return AccessType::SYSTEM_SHUTDOWN; }
    AccessRightsElements getRequiredAccessForDDLOnCluster() override
    {
        AccessRightsElements required_access;
        required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
        return required_access;
    }
    void executeImpl(BlockIO &) override
    {
        /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
        /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
        LOG_INFO(log, "Exit immediately as the SYSTEM KILL command has been issued.");
        _exit(128 + SIGKILL);
    }

    static const ASTSystemQuery::Type type = ASTSystemQuery::Type::KILL;
    static std::shared_ptr<ISystemCommand> create(SystemCommandFactory::Arguments & args)
    {
        return std::make_shared<KillCommand>(args.interpreter);
    }
};

class SuspendCommand final : public ISystemCommand
{
public:
    explicit SuspendCommand(InterpreterSystemQuery & interpreter_)
        : ISystemCommand(interpreter_)
    {
    }

    std::optional<AccessType> getAccess() override { return AccessType::SYSTEM_SHUTDOWN; }
    AccessRightsElements getRequiredAccessForDDLOnCluster() override
    {
        AccessRightsElements required_access;
        required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
        return required_access;
    }
    void executeImpl(BlockIO &) override
    {
        auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
        LOG_DEBUG(log, "Will run {}", command);
        auto res = ShellCommand::execute(command);
        res->in.close();
        WriteBufferFromOwnString out;
        copyData(res->out, out);
        copyData(res->err, out);
        if (!out.str().empty())
            LOG_DEBUG(log, "The command {} returned output: {}", command, out.str());
        res->wait();
    }

    static const ASTSystemQuery::Type type = ASTSystemQuery::Type::SUSPEND;
    static std::shared_ptr<ISystemCommand> create(SystemCommandFactory::Arguments & args)
    {
        return std::make_shared<SuspendCommand>(args.interpreter);
    }
};

void registerSystemCommands();
void registerSystemCommands()
{
    auto & factory = SystemCommandFactory::instance();

    factory.registerCommand<ShutdownCommand>();
    factory.registerCommand<KillCommand>();
    factory.registerCommand<SuspendCommand>();
}
}
