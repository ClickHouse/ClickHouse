#include <Common/Exception.h>
#include <Common/TerminalSize.h>
#include <Common/re2.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

#define EXTRACT_PATH_PATTERN ".*\\/store/(.*)"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/*
 * A tool to collect table data files on local fs as is (into current directory or into path from --output-dir option).
 * If test-mode option is added, files will be put by given url via PUT request.
 */

static void processFile(const fs::path & file_path, const fs::path & dst_path, bool test_mode, bool link, WriteBuffer & metadata_buf)
{
    String remote_path;
    RE2::FullMatch(file_path.string(), EXTRACT_PATH_PATTERN, &remote_path);
    bool is_directory = fs::is_directory(file_path);

    writeText(file_path.filename().string(), metadata_buf);
    writeChar('\t', metadata_buf);
    writeBoolText(is_directory, metadata_buf);
    if (!is_directory)
    {
        writeChar('\t', metadata_buf);
        writeIntText(fs::file_size(file_path), metadata_buf);
    }
    writeChar('\n', metadata_buf);

    if (is_directory)
        return;

    auto dst_file_path = fs::path(dst_path) / remote_path;

    if (link)
    {
        fs::create_symlink(file_path, dst_file_path);
    }
    else
    {
        ReadSettings read_settings{};
        read_settings.local_fs_method = LocalFSReadMethod::pread;
        auto src_buf = createReadBufferFromFileBase(file_path, read_settings, fs::file_size(file_path));
        std::shared_ptr<WriteBuffer> dst_buf;

        /// test mode for integration tests.
        if (test_mode)
            dst_buf = std::make_shared<WriteBufferFromHTTP>(HTTPConnectionGroupType::HTTP, Poco::URI(dst_file_path), Poco::Net::HTTPRequest::HTTP_PUT);
        else
            dst_buf = std::make_shared<WriteBufferFromFile>(dst_file_path);

        copyData(*src_buf, *dst_buf);
        dst_buf->next();
        dst_buf->finalize();
    }
}


static void processTableFiles(const fs::path & data_path, fs::path dst_path, bool test_mode, bool link)
{
    std::cerr << "Data path: " << data_path << ", destination path: " << dst_path << std::endl;

    String prefix;
    RE2::FullMatch(data_path.string(), EXTRACT_PATH_PATTERN, &prefix);

    std::shared_ptr<WriteBuffer> root_meta;
    if (test_mode)
    {
        dst_path /= "store";
        auto files_root = dst_path / prefix;
        root_meta = std::make_shared<WriteBufferFromHTTP>(HTTPConnectionGroupType::HTTP, Poco::URI(files_root / ".index"), Poco::Net::HTTPRequest::HTTP_PUT);
    }
    else
    {
        dst_path = fs::canonical(dst_path);
        auto files_root = dst_path / prefix;
        fs::create_directories(files_root);
        root_meta = std::make_shared<WriteBufferFromFile>(files_root / ".index");
    }

    fs::directory_iterator dir_end;
    for (fs::directory_iterator dir_it(data_path); dir_it != dir_end; ++dir_it)
    {
        if (dir_it->is_directory())
        {
            processFile(dir_it->path(), dst_path, test_mode, link, *root_meta);

            String directory_prefix;
            RE2::FullMatch(dir_it->path().string(), EXTRACT_PATH_PATTERN, &directory_prefix);

            std::shared_ptr<WriteBuffer> directory_meta;
            if (test_mode)
            {
                directory_meta = std::make_shared<WriteBufferFromHTTP>(HTTPConnectionGroupType::HTTP, Poco::URI(dst_path / directory_prefix / ".index"), Poco::Net::HTTPRequest::HTTP_PUT);
            }
            else
            {
                dst_path = fs::canonical(dst_path);
                fs::create_directories(dst_path / directory_prefix);
                directory_meta = std::make_shared<WriteBufferFromFile>(dst_path / directory_prefix / ".index");
            }

            fs::directory_iterator files_end;
            for (fs::directory_iterator file_it(dir_it->path()); file_it != files_end; ++file_it)
                processFile(file_it->path(), dst_path, test_mode, link, *directory_meta);

            directory_meta->next();
            directory_meta->finalize();
        }
        else
        {
            processFile(dir_it->path(), dst_path, test_mode, link, *root_meta);
        }
    }
    root_meta->next();
    root_meta->finalize();
}
}

int mainEntryClickHouseStaticFilesDiskUploader(int argc, char ** argv)
try
{
    using namespace DB;
    namespace po = boost::program_options;

    po::options_description description("Allowed options", getTerminalWidth());
    description.add_options()
        ("help,h", "produce help message")
        ("metadata-path", po::value<std::string>(), "Metadata path (SELECT data_paths FROM system.tables WHERE name = 'table_name' AND database = 'database_name')")
        ("test-mode", "Use test mode, which will put data on given url via PUT")
        ("link", "Create symlinks instead of copying")
        ("url", po::value<std::string>(), "Web server url for test mode")
        ("output-dir", po::value<std::string>(), "Directory to put files in non-test mode");

    po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
    po::variables_map options;
    po::store(parsed, options);
    po::notify(options);

    if (options.empty() || options.count("help"))
    {
        std::cout << description << std::endl;
        exit(0); // NOLINT(concurrency-mt-unsafe)
    }

    String metadata_path;

    if (options.count("metadata-path"))
        metadata_path = options["metadata-path"].as<std::string>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No metadata-path option passed");

    fs::path fs_path = fs::weakly_canonical(metadata_path);
    if (!fs::exists(fs_path))
    {
        std::cerr << fmt::format("Data path ({}) does not exist", fs_path.string());
        return 1;
    }

    String root_path;
    auto test_mode = options.contains("test-mode");
    if (test_mode)
    {
        if (options.count("url"))
            root_path = options["url"].as<std::string>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No url option passed for test mode");
    }
    else
    {
        if (options.count("output-dir"))
            root_path = options["output-dir"].as<std::string>();
        else
            root_path = fs::current_path();
    }

    processTableFiles(fs_path, root_path, test_mode, options.count("link"));

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(false) << '\n';
    return 1;
}
