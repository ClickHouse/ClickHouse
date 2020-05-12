#include "UDFConnector.h"

#include "FunctionUDF.h"


namespace DB
{

void UDFConnector::run()
{
    {
        std::unique_lock lock(mutex);
        if (!manager)
        {
            throw Exception("No manager", 0); /// @TODO Igr
        }
    }
    while (active)
    {
        UDFControlCommandResult cmd_result;
        cmd_result.read(manager->out);
        std::unique_lock lock(mutex);

        auto it = result_waiters.find(cmd_result.request_id);
        if (it == result_waiters.end())
            ; /// @TODO Igr Log Here
        else
        {
            it->second.result = std::move(cmd_result);
            it->second.done = true;
            it->second.cv.notify_all();
        }
    }
}

void UDFConnector::load(std::string_view filename)
{
    UDFControlCommand cmd;
    cmd.name = "InitLib";
    cmd.args = std::vector<std::string>{std::string(filename)};
    auto result = run_command(cmd);
    if (!result.isSuccess())
        throw Exception(result.message, result.code);

    std::string_view funcs(result.message);
    while (!funcs.empty())
    {
        auto pos = funcs.find(' ');
        auto name = std::string(funcs.substr(0, pos));
        funcs = funcs.substr(pos + 1 ? pos != std::string::npos : pos);
        if (!name.empty())
            FunctionFactory::instance().registerUserDefinedFunction(name, [=, this](const Context &) {
                return std::make_unique<DefaultOverloadResolver>(std::make_unique<FunctionUDF>(name, *this));
            });
    }
}

}

