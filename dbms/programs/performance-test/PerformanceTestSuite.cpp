#include <algorithm>
#include <iostream>
#include <limits>
#include <regex>
#include <thread>
#include <memory>
#include <filesystem>

#include <port/unistd.h>
#include <sys/stat.h>

#include <boost/program_options.hpp>

#include <Poco/Util/Application.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/XMLConfiguration.h>

#include <common/logger_useful.h>
#include <Client/Connection.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/UseSSL.h>
#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Common/InterruptListener.h>
#include <Common/TerminalSize.h>
#include <Common/StudentTTest.h>

#include "TestStats.h"
#include "ConfigPreprocessor.h"
#include "PerformanceTest.h"
#include "ReportBuilder.h"


namespace fs = std::filesystem;
namespace po = boost::program_options;

namespace DB
{

using Ports = std::vector<UInt16>;

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

/** Tests launcher for ClickHouse.
 * The tool walks through given or default folder in order to find files with
 * tests' descriptions and launches it.
 */
class PerformanceTestSuite
{
public:

    PerformanceTestSuite(
        Strings && hosts_,
        Ports && ports_,
        const bool secure_,
        const std::string & default_database_,
        const std::string & user_,
        const std::string & password_,
        const Settings & cmd_settings,
        const bool lite_output_,
        const std::string & profiles_file_,
        Strings && input_files_,
        Strings && tests_tags_,
        Strings && skip_tags_,
        Strings && tests_names_,
        Strings && skip_names_,
        Strings && tests_names_regexp_,
        Strings && skip_names_regexp_,
        std::unordered_map<std::string, std::vector<size_t>> && query_indexes_,
        const ConnectionTimeouts & timeouts_)
        : timeouts(timeouts_)
        , tests_tags(std::move(tests_tags_))
        , tests_names(std::move(tests_names_))
        , tests_names_regexp(std::move(tests_names_regexp_))
        , skip_tags(std::move(skip_tags_))
        , skip_names(std::move(skip_names_))
        , skip_names_regexp(std::move(skip_names_regexp_))
        , query_indexes(query_indexes_)
        , lite_output(lite_output_)
        , profiles_file(profiles_file_)
        , input_files(input_files_)
        , log(&Poco::Logger::get("PerformanceTestSuite"))
    {
        global_context.makeGlobalContext();
        global_context.getSettingsRef().copyChangesFrom(cmd_settings);
        if (input_files.empty())
            throw Exception("No tests were specified", ErrorCodes::BAD_ARGUMENTS);

        size_t connections_cnt = std::max(ports_.size(), hosts_.size());
        connections.reserve(connections_cnt);

        for (size_t i = 0; i < connections_cnt; ++i)
        {
            UInt16 cur_port = i >= ports_.size() ? 9000 : ports_[i];
            std::string cur_host = i >= hosts_.size() ? "localhost" : hosts_[i];
            connections.emplace_back(std::make_shared<Connection>(cur_host, cur_port, default_database_, user_, password_,
                    "performance-test-" + toString(i + 1),Protocol::Compression::Enable, secure_ ? Protocol::Secure::Enable : Protocol::Secure::Disable));
        }
        report_builder = std::make_shared<ReportBuilder>();
    }

    int run()
    {
        processTestsConfigurations();
        runTests();
        return 0;
    }

private:
    Connections connections;
    const ConnectionTimeouts & timeouts;
    const Strings & tests_tags;
    const Strings & tests_names;
    const Strings & tests_names_regexp;
    const Strings & skip_tags;
    const Strings & skip_names;
    const Strings & skip_names_regexp;
    std::unordered_map<std::string, std::vector<size_t>> query_indexes;

    Context global_context = Context::createGlobal();
    std::shared_ptr<ReportBuilder> report_builder;

    InterruptListener interrupt_listener;

    using XMLConfiguration = Poco::Util::XMLConfiguration;
    using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;

    bool lite_output;
    std::string profiles_file;

    Strings input_files;
    std::vector<XMLConfigurationPtr> tests_configurations;
    Poco::Logger * log;

    StudentTTest t_test;

    void processTestsConfigurations()
    {
        LOG_INFO(log, "Preparing test configurations");
        ConfigPreprocessor config_prep(input_files);
        tests_configurations = config_prep.processConfig(
            tests_tags,
            tests_names,
            tests_names_regexp,
            skip_tags,
            skip_names,
            skip_names_regexp);
        LOG_INFO(log, "Test configurations prepared");
    }

    void runTests()
    {
        if (!tests_configurations.empty())
        {
            Strings outputs;

            for (auto & test_config : tests_configurations)
            {
                auto [output, signal] = runTest(test_config);
                if (!output.empty())
                {
                    if (lite_output)
                        std::cout << output;
                    else
                        outputs.push_back(output);
                }
                if (signal)
                    break;
            }

            if (!lite_output && !outputs.empty())
            {
                std::cout << "[" << std::endl;

                for (size_t i = 0; i != outputs.size(); ++i)
                {
                    std::cout << outputs[i];
                    if (i != outputs.size() - 1)
                        std::cout << ",";

                    std::cout << std::endl;
                }

                std::cout << "]" << std::endl;
            }
        }
    }

    std::pair<std::string, bool> runTest(XMLConfigurationPtr & test_config)
    {
        PerformanceTestInfo info(test_config, profiles_file, global_context.getSettingsRef(), connections);
        LOG_INFO(log, "Config for test '" << info.test_name << "' parsed");
        PerformanceTest current(test_config, connections, timeouts, interrupt_listener, info, global_context, query_indexes[info.path], t_test);

        if (!current.checkPreconditions())
        {
            LOG_INFO(log, "Preconditions for test '" << info.test_name << "' are not fulfilled, skip run");
            return {"", current.checkSIGINT()};
        }

        LOG_INFO(log, "Preconditions for test '" << info.test_name << "' are fulfilled");
        LOG_INFO(log, "Preparing for run, have " << info.create_and_fill_queries.size() << " create and fill queries");
        current.prepare();
        LOG_INFO(log, "Prepared");
        LOG_INFO(log, "Running test '" << info.test_name << "'");
        std::vector<TestStats> result = current.execute();
        LOG_INFO(log, "Test '" << info.test_name << "' finished");
        LOG_INFO(log, "Running post run queries");
        current.finish();
        LOG_INFO(log, "Post run queries finished");

        if (lite_output)
            return {report_builder->buildCompactReport(info, result, query_indexes[info.path], connections), current.checkSIGINT()};

        return {report_builder->buildFullReport(info, result, query_indexes[info.path], connections, timeouts, t_test), current.checkSIGINT()};
    }
};

}

static void getFilesFromDir(const fs::path & dir, std::vector<std::string> & input_files, const bool recursive = false)
{
    Poco::Logger * log = &Poco::Logger::get("PerformanceTestSuite");
    if (dir.extension().string() == ".xml")
        LOG_WARNING(log, dir.string() + "' is a directory, but has .xml extension");

    fs::directory_iterator end;
    for (fs::directory_iterator it(dir); it != end; ++it)
    {
        const fs::path file = (*it);

        if (recursive && fs::is_directory(file))
            getFilesFromDir(file, input_files, recursive);
        else if (!fs::is_directory(file) && file.extension().string() == ".xml")
            input_files.push_back(file.string());
    }
}

static std::vector<std::string> getInputFiles(const po::variables_map & options, Poco::Logger * log)
{
    std::vector<std::string> input_files;
    bool recursive = options.count("recursive");

    if (!options.count("input-files"))
    {
        LOG_INFO(log, "Trying to find test scenario files in the current folder...");
        fs::path curr_dir(".");

        getFilesFromDir(curr_dir, input_files, recursive);

        if (input_files.empty())
            throw DB::Exception("Did not find any xml files", DB::ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        input_files = options["input-files"].as<std::vector<std::string>>();

        std::vector<std::string> collected_files;
        for (const std::string & filename : input_files)
        {
            fs::path file(filename);

            if (!fs::exists(file))
                throw DB::Exception("File '" + filename + "' does not exist", DB::ErrorCodes::FILE_DOESNT_EXIST);

            if (fs::is_directory(file))
                getFilesFromDir(file, collected_files, recursive);
            else
            {
                if (file.extension().string() != ".xml")
                    throw DB::Exception("File '" + filename + "' does not have .xml extension", DB::ErrorCodes::BAD_ARGUMENTS);

                collected_files.push_back(filename);
            }
        }

        input_files = std::move(collected_files);
    }

    LOG_INFO(log, "Found " + std::to_string(input_files.size()) + " input files");
    std::sort(input_files.begin(), input_files.end());
    return input_files;
}

static std::unordered_map<std::string, std::vector<std::size_t>> getTestQueryIndexes(const po::basic_parsed_options<char> & parsed_opts)
{
    std::unordered_map<std::string, std::vector<std::size_t>> result;
    const auto & options = parsed_opts.options;

    for (size_t i = 0; i + 1 < options.size(); ++i)
    {
        if (options[i].string_key == "input-files" && options[i + 1].string_key == "query-indexes")
        {
            const std::string & test_path = Poco::Path(options[i].value[0]).absolute().toString();
            for (const auto & query_num_str : options[i + 1].value)
            {
                size_t query_num = std::stoul(query_num_str);
                result[test_path].push_back(query_num);
            }
        }
    }
    return result;
}

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHousePerformanceTest(int argc, char ** argv)
try
{
    using po::value;
    using Strings = DB::Strings;
    using Ports = DB::Ports;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help", "produce help message")
        ("lite", "use lite version of output")
        ("profiles-file", value<std::string>()->default_value(""), "Specify a file with global profiles")
        ("host,h", value<Strings>()->multitoken(), "")
        ("port,p", value<Ports>()->multitoken(), "")
        ("secure,s", "Use TLS connection")
        ("database", value<std::string>()->default_value("default"), "")
        ("user", value<std::string>()->default_value("default"), "")
        ("password", value<std::string>()->default_value(""), "")
        ("log-level", value<std::string>()->default_value("information"), "Set log level")
        ("tags", value<Strings>()->multitoken(), "Run only tests with tag")
        ("skip-tags", value<Strings>()->multitoken(), "Do not run tests with tag")
        ("names", value<Strings>()->multitoken(), "Run tests with specific name")
        ("skip-names", value<Strings>()->multitoken(), "Do not run tests with name")
        ("names-regexp", value<Strings>()->multitoken(), "Run tests with names matching regexp")
        ("skip-names-regexp", value<Strings>()->multitoken(), "Do not run tests with names matching regexp")
        ("input-files", value<Strings>()->multitoken(), "Input .xml files")
        ("query-indexes", value<std::vector<size_t>>()->multitoken(), "Input query indexes")
        ("recursive,r", "Recurse in directories to find all xml's")
    ;

    DB::Settings cmd_settings;
    cmd_settings.addProgramOptions(desc);

    po::options_description cmdline_options;
    cmdline_options.add(desc);

    po::variables_map options;
    po::basic_parsed_options<char> parsed = po::command_line_parser(argc, argv).options(cmdline_options).run();
    auto queries_with_indexes = getTestQueryIndexes(parsed);
    po::store(parsed, options);

    po::notify(options);

    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %s: %t"));
    Poco::AutoPtr<Poco::ConsoleChannel> console_chanel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_chanel));

    Poco::Logger::root().setLevel(options["log-level"].as<std::string>());
    Poco::Logger::root().setChannel(channel);

    Poco::Logger * log = &Poco::Logger::get("PerformanceTestSuite");
    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options]\n";
        std::cout << desc << "\n";
        return 0;
    }

    Strings input_files = getInputFiles(options, log);

    Ports ports = options.count("port") ? options["port"].as<Ports>() : Ports({9000});
    Strings hosts = options.count("host") ? options["host"].as<Strings>() : Strings({"localhost"});

    Strings tests_tags = options.count("tags") ? options["tags"].as<Strings>() : Strings({});
    Strings skip_tags = options.count("skip-tags") ? options["skip-tags"].as<Strings>() : Strings({});
    Strings tests_names = options.count("names") ? options["names"].as<Strings>() : Strings({});
    Strings skip_names = options.count("skip-names") ? options["skip-names"].as<Strings>() : Strings({});
    Strings tests_names_regexp = options.count("names-regexp") ? options["names-regexp"].as<Strings>() : Strings({});
    Strings skip_names_regexp = options.count("skip-names-regexp") ? options["skip-names-regexp"].as<Strings>() : Strings({});

    auto timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(DB::Settings());

    DB::UseSSL use_ssl;

    DB::PerformanceTestSuite performance_test_suite(
        std::move(hosts),
        std::move(ports),
        options.count("secure"),
        options["database"].as<std::string>(),
        options["user"].as<std::string>(),
        options["password"].as<std::string>(),
        cmd_settings,
        options.count("lite"),
        options["profiles-file"].as<std::string>(),
        std::move(input_files),
        std::move(tests_tags),
        std::move(skip_tags),
        std::move(tests_names),
        std::move(skip_names),
        std::move(tests_names_regexp),
        std::move(skip_names_regexp),
        std::move(queries_with_indexes),
        timeouts);
    return performance_test_suite.run();
}
catch (...)
{
    std::cout << DB::getCurrentExceptionMessage(/*with stacktrace = */ true) << std::endl;
    int code = DB::getCurrentExceptionCode();
    return code ? code : 1;
}
