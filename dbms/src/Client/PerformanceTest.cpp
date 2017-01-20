#include <iostream>

#include <sys/stat.h>
#include <boost/program_options.hpp>

#include <DB/Common/ThreadPool.h>
#include <DB/Core/Types.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/Interpreters/Settings.h>
#include <Poco/AutoPtr.h>
#include <Poco/XML/XMLStream.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Exception.h>


/** Tests launcher for ClickHouse.
  * The tool walks through given or default folder in order to find files with
  * tests' description and launches it.
  */
namespace DB
{

namespace ErrorCodes
{
	extern const int POCO_EXCEPTION;
	extern const int STD_EXCEPTION;
	extern const int UNKNOWN_EXCEPTION;
}

class PerformanceTest
{
public:
	PerformanceTest(
		const unsigned   concurrency_,
		const String   & host_,
		const UInt16     port_,
		const String   & default_database_,
		const String   & user_,
		const String   & password_,
		const std::vector<std::string> & input_files,
        const std::vector<std::string> & tags,
        const std::vector<std::string> & without_tags,
        const std::vector<std::string> & names,
        const std::vector<std::string> & without_names,
        const std::vector<std::string> & names_regexp,
        const std::vector<std::string> & without_names_regexp,
		const Settings & settings_
	):
	   concurrency(concurrency_),
	   connections(concurrency, host_, port_, default_database_, user_, password_),
	   pool(concurrency),
	   testsConfigurations(input_files.size())
	{
		if (input_files.size() < 1) {
			throw Poco::Exception("No tests were specified", 1);
		}

		// std::cerr << std::fixed << std::setprecision(3);
		readTestsConfiguration(input_files);
	}

private:
	unsigned concurrency;
	ConnectionPool connections;
	Settings settings;
	ThreadPool pool;

	using XMLConfiguration = Poco::Util::XMLConfiguration;
	using Config = Poco::AutoPtr<XMLConfiguration>;
	std::vector<Config> testsConfigurations;

	void readTestsConfiguration(const std::vector<std::string> & input_files)
	{
		for (auto path : input_files) {
			// testsConfigurations.emplace_back(new XMLConfiguration(path));
			Poco::AutoPtr<XMLConfiguration> config(new XMLConfiguration(path));
			std::cout << config->getString("name") << std::endl;
		}

		// here will be tests filtering on tags, names, regexp matching, etc.
		// { ... }

		// for now let's launch one test only
		// if (testsConfigurations.size()) {
		// 	for (auto testConfig : testsConfigurations) {
		// 		runTest(testConfig);
		// 	}
		// }
	}

	// void runTest(const Config & testConfig)
	// {
	// 	std::cout << "Running: " << testConfig->getString("name") << "\n";

	// 	std::cout << "Check behaviour when attribute does not exist: " << testConfig->getString("someAttr") << "\n";
	// }
};

}

int mainEntryClickhousePerformanceTest(int argc, char ** argv) {
	using namespace DB;

	try
	{
		using boost::program_options::value;
		using Strings = std::vector<std::string>;

		boost::program_options::options_description desc("Allowed options");
		desc.add_options()
			("help", 																	"produce help message")
			("concurrency,c",		value<unsigned>()->default_value(1),				"number of parallel queries")
			("host,h",				value<std::string>()->default_value("localhost"),	"")
			("port", 				value<UInt16>()->default_value(9000),				"")
			("user", 				value<std::string>()->default_value("default"),		"")
			("password",			value<std::string>()->default_value(""),			"")
			("database",			value<std::string>()->default_value("default"),		"")
			("tag",					value<Strings>(),									"Run only tests with tag")
			("without-tag",			value<Strings>(),									"Do not run tests with tag")
			("name",				value<Strings>(),									"Run tests with specific name")
			("without-name",		value<Strings>(),									"Do not run tests with name")
			("name-regexp",			value<Strings>(),									"Run tests with names matching regexp")
			("without-name-regexp",	value<Strings>(),									"Do not run tests with names matching regexp")

		#define DECLARE_SETTING(TYPE, NAME, DEFAULT) (#NAME, boost::program_options::value<std::string> (), "Settings.h")
		#define   DECLARE_LIMIT(TYPE, NAME, DEFAULT) (#NAME, boost::program_options::value<std::string> (), "Limits.h")
			APPLY_FOR_SETTINGS(DECLARE_SETTING)
			APPLY_FOR_LIMITS(DECLARE_LIMIT)
		#undef DECLARE_SETTING
		#undef DECLARE_LIMIT
		;

		/// These options will not be displayed in --help
		boost::program_options::options_description hidden("Hidden options");
		hidden.add_options()
			("input-files", value< std::vector<std::string> >(), "")
			;

		/// But they will be legit, though. And they must be given without name
		boost::program_options::positional_options_description positional;
		positional.add("input-files", -1);

		boost::program_options::options_description cmdline_options;
		cmdline_options.add(desc).add(hidden);

		boost::program_options::variables_map options;
		boost::program_options::store(
			boost::program_options::command_line_parser(argc, argv)
									.options(cmdline_options)
									.positional(positional)
									.run(),
			options
		);
		boost::program_options::notify(options);

		if (options.count("help"))
		{
			std::cout << "Usage: " << argv[0] << " [options] [test_file ...] [tests_folder]\n";
			std::cout << desc << "\n";
			return 1;
		}

		if (! options.count("input-files")) {
			std::cerr << "No tests files were specified. See --help" << "\n";
			return 1;
		}

		/// Extract settings and limits from given options
		Settings settings;

		#define EXTRACT_SETTING(TYPE, NAME, DEFAULT) \
			if (options.count(#NAME)) \
				settings.set(#NAME, options[#NAME].as<std::string>());
			APPLY_FOR_SETTINGS(EXTRACT_SETTING)
			APPLY_FOR_LIMITS(EXTRACT_SETTING)
		#undef EXTRACT_SETTING

		Strings tests_tags;
		Strings skip_tags;
		Strings tests_names;
		Strings skip_names;
		Strings name_regexp;
		Strings skip_matching_regexp;

		if (options.count("tag")) {
			tests_tags = options["tag"].as<Strings>();
		}

		if (options.count("without-tag")) {
			skip_tags = options["without-tag"].as<Strings>();
		}

		if (options.count("name")) {
			tests_names = options["name"].as<Strings>();
		}

		if (options.count("without-name")) {
			skip_names = options["without-name"].as<Strings>();
		}

		if (options.count("name-regexp")) {
			name_regexp = options["name-regexp"].as<Strings>();
		}

		if (options.count("without-name-regexp")) {
			skip_matching_regexp = options["without-name-regexp"].as<Strings>();
		}



		PerformanceTest performanceTest(
			options["concurrency"].as<unsigned>(),
			options["host"       ].as<std::string>(),
			options["port"       ].as<UInt16>(),
			options["database"   ].as<std::string>(),
			options["user"       ].as<std::string>(),
			options["password"   ].as<std::string>(),
			options["input-files"].as<Strings>(),
			tests_tags,
			skip_tags,
			tests_names,
			skip_names,
			name_regexp,
			skip_matching_regexp,
			settings
		);
	}
	catch (const Exception & e)
	{
		std::string text = e.displayText();

		std::cerr << "Code: " << e.code() << ". " << text << "\n\n";

		/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
		if (std::string::npos == text.find("Stack trace"))
			std::cerr << "Stack trace:\n"
				<< e.getStackTrace().toString();

		return e.code();
	}
	catch (const Poco::Exception & e)
	{
		std::cerr << "Poco::Exception: " << e.displayText() << "\n";
		return ErrorCodes::POCO_EXCEPTION;
	}
	catch (const std::exception & e)
	{
		std::cerr << "std::exception: " << e.what() << "\n";
		return ErrorCodes::STD_EXCEPTION;
	}
	catch (...)
	{
		std::cerr << "Unknown exception\n";
		return ErrorCodes::UNKNOWN_EXCEPTION;
	}

	return 0;
}
