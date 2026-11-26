#include <Common/StringUtils.h>
#include <Disks/DiskLocal.h>
#include <IO/PackedFilesOperations.h>
#include <Interpreters/Context.h>
#include <Common/TerminalSize.h>
#include <ICommand.h>
#include <base/types.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/split.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace po = boost::program_options;

class CommandPackedIO final : public ICommand
{
public:
    CommandPackedIO() : ICommand("CommandPackedIO")
    {
        command_name = "packed-io";
        description = "List/Extracts/Creates archive in ClickHouse packed format";

        // options_description.emplace(createOptionsDescription("Allowed options", getTerminalWidth()));

        options_description.add_options()("recursive", "Extract/Create packed archive recursively");
        options_description.add_options()(
            "disk-from", po::value<String>(), "Name of the disk of input file/directory. By default the same as --disk");
        options_description.add_options()(
            "disk-to",
            po::value<String>(),
            "Name of the disk of output file/directory. By default local disk with root as working directory of running program");
        options_description.add_options()(
            "file-order",
            po::value<String>(),
            "File order hint for archive creation. The files listed in the hint will be placed in the archive in the order they are listed."
            " The hint is a space-separated list of file names. The files that are not listed in the hint will be placed after the files "
            "listed in the hint.");
        options_description.add_options()("command", "Command for packed-io to execute (mandatory, positional)");
        options_description.add_options()("path-from", "Path from (mandatory, positional)");
        options_description.add_options()("path-to", "Path to (mandatory for commands `extract` and `create`, positional)");
        positional_options_description.add("command", 1);
        positional_options_description.add("path-from", 1);
        positional_options_description.add("path-to", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        auto disk_from = getDiskWithPath(client, options, "disk-from");
        auto disk_to = getDiskWithPath(client, options, "disk-to");
        String command = getValueFromCommandLineOptionsThrow<String>(options, "command");
        String path_from = disk_from.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));
        std::optional<String> path_to = getValueFromCommandLineOptionsWithOptional<String>(options, "path-to");
        bool recursive = options.contains("recursive");
        String file_order = getValueFromCommandLineOptionsWithDefault<String>(options, "file-order", "");

        if (command == "extract" || command == "create")
        {
            if (!path_to.has_value())
            {
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS, "For commands `extract` and `create` destination path should be specified");
            }
        }

        if (command == "list")
        {
            if (recursive)
            {
                LOG_INFO(log, "Listing packed files recursively on disk {}, path {}", disk_from.getDisk()->getName(), path_from);
                auto listings = listPackedRecursive(disk_from.getDisk(), path_from);
                for (const auto & [path, listing] : listings)
                    printListing(path.string(), listing, std::cout);
            }
            else
            {
                LOG_INFO(log, "Listing packed files on disk {}, path {}", disk_from.getDisk()->getName(), path_from);
                auto listing = listPacked(disk_from.getDisk(), path_from);
                printListing("", listing, std::cout);
            }
        }
        else if (command == "extract")
        {
            String output_path = disk_to.getRelativeFromRoot(path_to.value());

            if (recursive)
            {
                LOG_INFO(
                    log,
                    "Extracting packed files recursively from disk {}, path {}, to disk {}, path {}",
                    disk_from.getDisk()->getName(),
                    path_from,
                    disk_to.getDisk()->getName(),
                    output_path);
                extractPackedRecursive(disk_from.getDisk(), path_from, disk_to.getDisk(), output_path);
            }
            else
            {
                LOG_INFO(
                    log,
                    "Extracting packed files from disk {}, path {}, to disk {}, path {}",
                    disk_from.getDisk()->getName(),
                    path_from,
                    disk_to.getDisk()->getName(),
                    output_path);
                extractPacked(disk_from.getDisk(), path_from, disk_to.getDisk(), output_path);
            }
        }
        else if (command == "create")
        {
            String output_path = disk_to.getRelativeFromRoot(path_to.value());
            Strings files_order_hint;
            boost::split(files_order_hint, file_order, isWhitespaceASCII);

            if (recursive)
            {
                LOG_INFO(
                    log,
                    "Creating packed archive recursively from disk {}, path {}, to disk {}, path {}",
                    disk_from.getDisk()->getName(),
                    path_from,
                    disk_to.getDisk()->getName(),
                    output_path);
                createPackedRecursive(disk_from.getDisk(), path_from, disk_to.getDisk(), output_path, files_order_hint);
            }
            else
            {
                LOG_INFO(
                    log,
                    "Creating packed archive from disk {}, path {}, to disk {}, path {}",
                    disk_from.getDisk()->getName(),
                    path_from,
                    disk_to.getDisk()->getName(),
                    output_path);
                createPacked(disk_from.getDisk(), path_from, disk_to.getDisk(), output_path, files_order_hint);
            }
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown subcommand for packed-io");
        }
    }
};


CommandPtr makeCommandPackedIO()
{
    return std::make_shared<DB::CommandPackedIO>();
}

}
