#include "ICommand.h"
#include "DisksClient.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CommandLineOptions ICommand::processCommandLineArguments(const Strings & commands)
{
    CommandLineOptions options;
    auto parser = po::command_line_parser(commands);
    parser.options(options_description).positional(positional_options_description);

    po::parsed_options parsed = parser.run();
    po::store(parsed, options);

    return options;
}

void ICommand::execute(const Strings & commands, DisksClient & client)
{
    try
    {
        processCommandLineArguments(commands);
    }
    catch (std::exception & exc)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", exc.what());
    }
    executeImpl(processCommandLineArguments(commands), client);
}

DiskWithPath & ICommand::getDiskWithPath(DisksClient & client, const CommandLineOptions & options, const String & name)
{
    auto disk_name = getValueFromCommandLineOptionsWithOptional<String>(options, name);
    if (disk_name.has_value())
        return client.getDiskWithPath(disk_name.value());

    return client.getCurrentDiskWithPath();
}

}
