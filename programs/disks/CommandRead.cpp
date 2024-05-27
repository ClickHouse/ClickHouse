#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include "ICommand.h"

namespace DB
{

class CommandRead final : public ICommand
{
public:
    CommandRead()
    {
        command_name = "read";
        description = "Read a file from `FROM_PATH` to `TO_PATH`";
        options_description.add_options()(
            "path-from", po::value<String>(), "file from which we are reading, defaults to `stdin` (mandatory, positional)")(
            "path-to", po::value<String>(), "file to which we are writing");
        positional_options_description.add("path-from", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath();
        String path_from = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        std::optional<String> path_to = getValueFromCommandLineOptionsWithOptional<String>(options, "path-to");
        if (path_to.has_value())
        {
            path_to = std::optional{disk.getRelativeFromRoot(path_to.value())};
        }

        auto in = disk.getDisk()->readFile(path_from);
        if (path_to.has_value())
        {
            String relative_path_to = disk.getRelativeFromRoot(path_to.value());

            auto out = disk.getDisk()->writeFile(relative_path_to);
            copyData(*in, *out);
            out->finalize();
        }
        else
        {
            std::unique_ptr<WriteBufferFromFileBase> out = std::make_unique<WriteBufferFromFileDescriptor>(STDOUT_FILENO);
            copyData(*in, *out);
        }
    }
};

CommandPtr makeCommandRead()
{
    return std::make_unique<DB::CommandRead>();
}

}
