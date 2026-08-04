#include <Common/StringUtils.h>
#include <Disks/DiskLocal.h>
#include <IO/PackedFilesOperations.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string/split.hpp>
#include <filesystem>
#include <iostream>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace po = boost::program_options;

using namespace DB;

int mainEntryClickHousePackedIO(int argc, char ** argv);
int mainEntryClickHousePackedIO(int argc, char ** argv)
try
{
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("list,l", "list packed archive")
        ("extract,x", "extract packed archive")
        ("create,c", "create packed archive")
        ("input,i", po::value<String>()->required(), "input file/directory")
        ("output,o", po::value<String>(), "output file/directory (required for --extract and --create)")
        ("file-order", po::value<String>(), "File order hint for archive creation. The files listed in the hint will be placed in the archive in the order they are listed."
            " The hint is a space-separated list of file names. The files that are not listed in the hint will be placed after the files listed in the hint.")
        ("recursive,r", R"(list/extract/create packed archive recursively.
Recursive list traverses input directory and subdirectories, lists files inside archives with '.packed' extension, lists other files as is.
Recursive extract traverses input directory and subdirectories, unpacks files with '.packed' extension to the same directory, copies other files as is.
Recursive create traverses input directory and subdirectories, collects all files in directory and writes them into packed archive with name 'data.packed'.
)");

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    if (options.contains("help"))
    {
        std::cout << "Lists/Extracts/Creates archive in ClickHouse packed format.\n";
        std::cout << "Usage: packed-io -i <from_path> -o <to_path> {--list |--extract | --create} [--recursive] [--file-order 'file1 file2 dir1/file3 ...']\n\n";
        std::cout << "E.g. if you want to unpack data part from path '/data/default/table/all_1_1_0' to current directory, you can run:\n";
        std::cout << "packed-io -i /data/default/table/all_1_1_0 -o . --extract --recursive\n\n";
        std::cout << desc << std::endl;
        return 1;
    }

    po::notify(options);

    if (options.count("extract") + options.count("create") + options.count("list") != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Exactly one of --list, --extract or --create must be specified");

    auto input_path = options.at("input").as<String>();

    String output_path;
    if (options.contains("extract") || options.contains("create"))
    {
        if (!options.contains("output"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "--extract and --create require an output path (--output/-o)");
        output_path = options.at("output").as<String>();
    }

    bool recursive = options.contains("recursive");
    auto disk = std::make_shared<DiskLocal>("tmp_local_disk", "./");

    if (options.contains("list"))
    {
        if (recursive)
        {
            auto listings = listPackedRecursive(disk, input_path);
            for (const auto & [path, listing] : listings)
                printListing(path.string(), listing, std::cout);
        }
        else
        {
            auto listing = listPacked(disk, input_path);
            printListing("", listing, std::cout);
        }
    }
    else if (options.contains("extract"))
    {
        if (recursive)
            extractPackedRecursive(disk, input_path, disk, output_path);
        else
            extractPacked(disk, input_path, disk, output_path);
    }
    else if (options.contains("create"))
    {
        auto file_order_hint_str = options.contains("file-order") ? options.at("file-order").as<String>() : String();
        Strings files_order_hint;
        boost::split(files_order_hint, file_order_hint_str, isWhitespaceASCII);

        if (recursive)
            createPackedRecursive(disk, input_path, disk, output_path, files_order_hint);
        else
            createPacked(disk, input_path, disk, output_path, files_order_hint);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required either 'list' or 'extract' or 'create' option");

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    return 1;
}
