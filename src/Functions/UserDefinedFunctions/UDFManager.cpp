#include "UDFManager.h"

namespace DB
{

UDFControlCommandResult UDFManager::initLib(const std::string & filename) {
    UDFLib lib(filename);
    UDFControlCommandResult result = lib.load();
    if (result.isSuccess())
    {
        std::string name = std::string(lib.getName());
        if (libs.find(filename) != libs.end())
        {
            result.code = 1;
            result.message = "Lib " + name + " already exists";
        }
        else
            libs.insert({std::move(name), std::move(lib)});
    }
    return result;
}

int UDFManager::run()
{
    UDFControlCommand command;
    UDFControlCommandResult result;
    bool active = true;
    while (active)
    {
        result.message.clear();
        result.code = 0;
        command.read(in);
        if (command.name == UDFControlCommand::InitLib)
        {
            /// Arg is filename
            if (command.args.size() != 1) {
                result.code = 1;
                result.message = "Wrong arguments";
            } else {
                result = initLib(command.args[0]);
            }
        }
        else if (command.name == UDFControlCommand::ExecFunc)
        {
            /// Args is data_pointer and library_name + "_" + function_name
            ;
        }
        else if (command.name == UDFControlCommand::Stop)
        {
            active = false;
        }
        result.request_id = command.request_id;
        result.write(out);
    }
    return 0;
}

}
