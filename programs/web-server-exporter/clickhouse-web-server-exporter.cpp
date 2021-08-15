#include <Common/Exception.h>
#include <Common/TerminalSize.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/copyData.h>
#include <IO/createReadBufferFromFileBase.h>

#include <boost/program_options.hpp>
#include <re2/re2.h>
#include <filesystem>

namespace fs = std::filesystem;

#define UUID_PATTERN "[\\w]{8}-[\\w]{4}-[\\w]{4}-[\\w]{4}-[\\w]{12}"
#define EXTRACT_UUID_PATTERN fmt::format(".*/({})/.*", UUID_PATTERN)


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


void processTableFiles(const String & url, const fs::path & path, const String & files_prefix, String uuid)
{
    WriteBufferFromHTTP metadata_buf(Poco::URI(fs::path(url) / (".index-" + uuid)), Poco::Net::HTTPRequest::HTTP_PUT);
    fs::directory_iterator dir_end;

    auto process_file = [&](const String & file_name, const String & file_path)
    {
        auto remote_file_name = files_prefix + "-" + uuid + "-" + file_name;
        writeText(remote_file_name, metadata_buf);
        writeChar('\t', metadata_buf);
        writeIntText(fs::file_size(file_path), metadata_buf);
        writeChar('\n', metadata_buf);

        auto src_buf = createReadBufferFromFileBase(file_path, fs::file_size(file_path), 0, 0, nullptr);
        WriteBufferFromHTTP dst_buf(Poco::URI(fs::path(url) / remote_file_name), Poco::Net::HTTPRequest::HTTP_PUT);

        copyData(*src_buf, dst_buf);
        dst_buf.next();
        dst_buf.finalize();
    };

    for (fs::directory_iterator dir_it(path); dir_it != dir_end; ++dir_it)
    {
        if (dir_it->is_directory())
        {
            fs::directory_iterator files_end;
            for (fs::directory_iterator file_it(dir_it->path()); file_it != files_end; ++file_it)
            {
                process_file(dir_it->path().filename().string() + "-" + file_it->path().filename().string(), file_it->path());
            }
        }
        else
        {
            process_file(dir_it->path().filename(), dir_it->path());
        }
    }

    metadata_buf.next();
    metadata_buf.finalize();
}

}


int mainEntryClickHouseWebServerExporter(int argc, char ** argv)
try
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description description("Allowed options", getTerminalWidth());
    description.add_options()
        ("help,h", "produce help message")
        ("metadata-path", po::value<std::string>(), "Metadata path (select data_paths from system.tables where name='table_name'")
        ("url", po::value<std::string>(), "Web server url")
        ("files-prefix", po::value<std::string>(), "Prefix for stored files");

    po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
    po::variables_map options;
    po::store(parsed, options);
    po::notify(options);

    if (options.empty() || options.count("help"))
    {
        std::cout << description << std::endl;
        exit(0);
    }

    String url, metadata_path, files_prefix;

    if (options.count("metadata-path"))
        metadata_path = options["metadata-path"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No metadata-path option passed");

    if (options.count("files-prefix"))
        files_prefix = options["files-prefix"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No files-prefix option passed");

    fs::path fs_path = fs::weakly_canonical(metadata_path);
    if (!fs::exists(fs_path))
    {
        std::cerr << fmt::format("Data path ({}) does not exist", fs_path.string());
        return 1;
    }

    String uuid;
    if (!RE2::Extract(metadata_path, EXTRACT_UUID_PATTERN, "\\1", &uuid))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot extract uuid for: {}", metadata_path);

    if (options.count("url"))
        url = options["url"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No url option passed");

    processTableFiles(url, fs_path, files_prefix, uuid);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true);
    return 1;
}
