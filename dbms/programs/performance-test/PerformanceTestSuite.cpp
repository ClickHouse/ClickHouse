#include <functional>
#include <iostream>
#include <limits>
#include <regex>
#include <thread>
#include <port/unistd.h>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <sys/stat.h>


#include <common/logger_useful.h>
#include <common/DateLUT.h>
#include <AggregateFunctions/ReservoirSampler.h>
#include <Client/Connection.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Core/Types.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/UseSSL.h>
#include <Interpreters/Settings.h>
#include <common/ThreadPool.h>
#include <common/getMemoryAmount.h>
#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLStream.h>
#include <Poco/Util/Application.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Common/InterruptListener.h>
#include <Common/Config/configReadClient.h>

#include "JSONString.h"
#include "StopConditionsSet.h"
#include "TestStopConditions.h"
#include "TestStats.h"
#include "ConfigPreprocessor.h"
#include "PerformanceTest.h"
#include "ReportBuilder.h"

#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif


/** Tests launcher for ClickHouse.
  * The tool walks through given or default folder in order to find files with
  * tests' descriptions and launches it.
  */
namespace fs = boost::filesystem;
using String = std::string;

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

class PerformanceTestSuite : public Poco::Util::Application
{
public:
    using Strings = std::vector<String>;

    PerformanceTestSuite(const String & host_,
        const UInt16 port_,
        const bool secure_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const bool lite_output_,
        const String & profiles_file_,
        Strings && input_files_,
        Strings && tests_tags_,
        Strings && skip_tags_,
        Strings && tests_names_,
        Strings && skip_names_,
        Strings && tests_names_regexp_,
        Strings && skip_names_regexp_,
        const ConnectionTimeouts & timeouts)
        : connection(host_, port_, default_database_, user_, password_, timeouts, "performance-test", Protocol::Compression::Enable, secure_ ? Protocol::Secure::Enable : Protocol::Secure::Disable),
          lite_output(lite_output_),
          profiles_file(profiles_file_),
          input_files(input_files_),
          tests_tags(std::move(tests_tags_)),
          skip_tags(std::move(skip_tags_)),
          tests_names(std::move(tests_names_)),
          skip_names(std::move(skip_names_)),
          tests_names_regexp(std::move(tests_names_regexp_)),
          skip_names_regexp(std::move(skip_names_regexp_))
    {
        if (input_files.size() < 1)
        {
            throw DB::Exception("No tests were specified", DB::ErrorCodes::BAD_ARGUMENTS);
        }
    }

    void initialize(Poco::Util::Application & self [[maybe_unused]])
    {
        std::string home_path;
        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;
        configReadClient(Poco::Util::Application::instance().config(), home_path);
    }

    int main(const std::vector < std::string > & /* args */)
    {
        std::string name;
        UInt64 version_major;
        UInt64 version_minor;
        UInt64 version_patch;
        UInt64 version_revision;
        std::cerr << "IN APP\n";
        connection.getServerVersion(name, version_major, version_minor, version_patch, version_revision);

        std::stringstream ss;
        ss << version_major << "." << version_minor << "." << version_patch;
        server_version = ss.str();
        std::cerr << "SErver version:" << server_version << std::endl;

        report_builder = std::make_shared<ReportBuilder>(server_version);
        std::cerr << "REPORT BUILDER created\n";

        processTestsConfigurations(input_files);

        return 0;
    }

private:
    const Strings & tests_tags;
    const Strings & tests_names;
    const Strings & tests_names_regexp;
    const Strings & skip_tags;
    const Strings & skip_names;
    const Strings & skip_names_regexp;

    Context global_context = Context::createGlobal();
    std::shared_ptr<ReportBuilder> report_builder;

    Connection connection;
    std::string server_version;

    InterruptListener interrupt_listener;

    using XMLConfiguration = Poco::Util::XMLConfiguration;
    using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;

    bool lite_output;
    String profiles_file;

    Strings input_files;
    std::vector<XMLConfigurationPtr> tests_configurations;

    void processTestsConfigurations(const std::vector<std::string> & paths)
    {
        ConfigPreprocessor config_prep(paths);
        std::cerr << "CONFIG CREATED\n";
        tests_configurations = config_prep.processConfig(
            tests_tags,
            tests_names,
            tests_names_regexp,
            skip_tags,
            skip_names,
            skip_names_regexp);

        std::cerr << "CONFIGURATIONS RECEIVED\n";
        if (tests_configurations.size())
        {
            Strings outputs;

            for (auto & test_config : tests_configurations)
            {
                std::cerr << "RUNNING TEST\n";
                String output = runTest(test_config);
                if (lite_output)
                    std::cout << output;
                else
                    outputs.push_back(output);
            }

            if (!lite_output && outputs.size())
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

    String runTest(XMLConfigurationPtr & test_config)
    {
        //test_name = test_config->getString("name");
        //std::cerr << "Running: " << test_name << "\n";

        std::cerr << "RUNNING TEST really\n";
        PerformanceTestInfo info(test_config, profiles_file);
        std::cerr << "INFO CREATED\n";
        PerformanceTest current(test_config, connection, interrupt_listener, info, global_context);
        std::cerr << "Checking preconditions\n";
        current.checkPreconditions();

        std::cerr << "Executing\n";
        auto result = current.execute();

        if (lite_output)
            return report_builder->buildCompactReport(info, result);
        else
            return report_builder->buildFullReport(info, result);
    }

};
}

static void getFilesFromDir(const fs::path & dir, std::vector<String> & input_files, const bool recursive = false)
{
    if (dir.extension().string() == ".xml")
        std::cerr << "Warning: '" + dir.string() + "' is a directory, but has .xml extension" << std::endl;

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


int mainEntryClickHousePerformanceTest(int argc, char ** argv)
try
{
    using boost::program_options::value;
    using Strings = std::vector<String>;

    Poco::Logger::root().setLevel("information");
    Poco::Logger::root().setChannel(new Poco::FormattingChannel(new Poco::PatternFormatter("%Y.%m.%d %H:%M:%S.%F <%p> %t"), new Poco::ConsoleChannel));
    Poco::Logger * log = &Poco::Logger::get("PerformanceTestSuite");

    std::cerr << "HELLO\n";
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help", "produce help message")
        ("lite", "use lite version of output")
        ("profiles-file", value<String>()->default_value(""), "Specify a file with global profiles")
        ("host,h", value<String>()->default_value("localhost"), "")
        ("port", value<UInt16>()->default_value(9000), "")
        ("secure,s", "Use TLS connection")
        ("database", value<String>()->default_value("default"), "")
        ("user", value<String>()->default_value("default"), "")
        ("password", value<String>()->default_value(""), "")
        ("tags", value<Strings>()->multitoken(), "Run only tests with tag")
        ("skip-tags", value<Strings>()->multitoken(), "Do not run tests with tag")
        ("names", value<Strings>()->multitoken(), "Run tests with specific name")
        ("skip-names", value<Strings>()->multitoken(), "Do not run tests with name")
        ("names-regexp", value<Strings>()->multitoken(), "Run tests with names matching regexp")
        ("skip-names-regexp", value<Strings>()->multitoken(), "Do not run tests with names matching regexp")
        ("recursive,r", "Recurse in directories to find all xml's");

    /// These options will not be displayed in --help
    boost::program_options::options_description hidden("Hidden options");
    hidden.add_options()
        ("input-files", value<std::vector<String>>(), "");

    /// But they will be legit, though. And they must be given without name
    boost::program_options::positional_options_description positional;
    positional.add("input-files", -1);

    boost::program_options::options_description cmdline_options;
    cmdline_options.add(desc).add(hidden);

    boost::program_options::variables_map options;
    boost::program_options::store(
        boost::program_options::command_line_parser(argc, argv).options(cmdline_options).positional(positional).run(), options);
    boost::program_options::notify(options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options] [test_file ...] [tests_folder]\n";
        std::cout << desc << "\n";
        return 0;
    }

    Strings input_files;
    bool recursive = options.count("recursive");

    if (!options.count("input-files"))
    {
        LOG_INFO(log, "Trying to find test scenario files in the current folder...");
        fs::path curr_dir(".");

        getFilesFromDir(curr_dir, input_files, recursive);

        if (input_files.empty())
        {
            std::cerr << std::endl;
            throw DB::Exception("Did not find any xml files", DB::ErrorCodes::BAD_ARGUMENTS);
        }
        else
            std::cerr << " found " << input_files.size() << " files." << std::endl;
    }
    else
    {
        std::cerr << "WOLRD\n";
        input_files = options["input-files"].as<Strings>();
        LOG_INFO(log, "Found " + std::to_string(input_files.size()) + " input files");
        Strings collected_files;

        for (const String & filename : input_files)
        {
            fs::path file(filename);

            if (!fs::exists(file))
                throw DB::Exception("File '" + filename + "' does not exist", DB::ErrorCodes::FILE_DOESNT_EXIST);

            if (fs::is_directory(file))
            {
                getFilesFromDir(file, collected_files, recursive);
            }
            else
            {
                if (file.extension().string() != ".xml")
                    throw DB::Exception("File '" + filename + "' does not have .xml extension", DB::ErrorCodes::BAD_ARGUMENTS);
                collected_files.push_back(filename);
            }
        }

        input_files = std::move(collected_files);
    }

    Strings tests_tags = options.count("tags") ? options["tags"].as<Strings>() : Strings({});
    Strings skip_tags = options.count("skip-tags") ? options["skip-tags"].as<Strings>() : Strings({});
    Strings tests_names = options.count("names") ? options["names"].as<Strings>() : Strings({});
    Strings skip_names = options.count("skip-names") ? options["skip-names"].as<Strings>() : Strings({});
    Strings tests_names_regexp = options.count("names-regexp") ? options["names-regexp"].as<Strings>() : Strings({});
    Strings skip_names_regexp = options.count("skip-names-regexp") ? options["skip-names-regexp"].as<Strings>() : Strings({});

    auto timeouts = DB::ConnectionTimeouts::getTCPTimeoutsWithoutFailover(DB::Settings());

    DB::UseSSL use_ssl;

    LOG_INFO(log, "Running something");
    DB::PerformanceTestSuite performance_test(
        options["host"].as<String>(),
        options["port"].as<UInt16>(),
        options.count("secure"),
        options["database"].as<String>(),
        options["user"].as<String>(),
        options["password"].as<String>(),
        options.count("lite") > 0,
        options["profiles-file"].as<String>(),
        std::move(input_files),
        std::move(tests_tags),
        std::move(skip_tags),
        std::move(tests_names),
        std::move(skip_names),
        std::move(tests_names_regexp),
        std::move(skip_names_regexp),
        timeouts);
    std::cerr << "TEST CREATED\n";
    return performance_test.run();
}
catch (...)
{
    std::cout << DB::getCurrentExceptionMessage(/*with stacktrace = */ true) << std::endl;
    int code = DB::getCurrentExceptionCode();
    return code ? code : 1;
}
