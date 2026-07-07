#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <ICommand.h>
#include <Common/logger_useful.h>

namespace DB
{

class CommandRead final : public ICommand
{
public:
    CommandRead() : ICommand("CommandRead")
    {
        command_name = "read";
        description = "Read a file from `path-from` to `path-to`";
        options_description.add_options()("path-from", po::value<String>(), "file from which we are reading (mandatory, positional)")(
            "path-to", po::value<String>(), "file to which we are writing, defaults to `stdout`");
        positional_options_description.add("path-from", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto & disk = client.getCurrentDiskWithPath();
        String path_from = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        std::optional<String> path_to = getValueFromCommandLineOptionsWithOptional<String>(options, "path-to");

        auto in = disk.getDisk()->readFile(path_from, getReadSettings());
        std::unique_ptr<WriteBufferFromFileBase> out = {};
        if (path_to.has_value())
        {
            String relative_path_to = disk.getRelativeFromRoot(path_to.value());
            out = disk.getDisk()->writeFile(relative_path_to);
            LOG_INFO(log, "Writing file from '{}' to '{}' at disk '{}'", path_from, path_to.value(), disk.getDisk()->getName());
            copyData(*in, *out);
        }
        else
        {
            out = std::make_unique<WriteBufferFromFileDescriptor>(STDOUT_FILENO);
            LOG_INFO(log, "Writing file from '{}' to 'stdout' at disk '{}'", path_from, disk.getDisk()->getName());
            copyData(*in, *out);
            out->write('\n');
        }
        out->finalize();
    }
};

CommandPtr makeCommandRead()
{
    return std::make_shared<DB::CommandRead>();
}

}
