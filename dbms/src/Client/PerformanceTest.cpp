#include <iostream>
#include <limits>

#include <sys/stat.h>
#include <boost/program_options.hpp>

#include <DB/Client/ConnectionPool.h>
#include <DB/Common/ConcurrentBoundedQueue.h>
#include <DB/Common/Stopwatch.h>
#include <DB/Common/ThreadPool.h>
#include <DB/Core/Types.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/Settings.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>


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

struct criterionWithPriority {
	std::string priority = "";
	size_t      value    = 0;
};

/// Termination criterions. The running test will be terminated in either of two conditions:
/// 1. All criterions marked 'min' are fulfilled
/// or
/// 2. Any criterion  marked 'max' is  fulfilled
class StopCriterions {
private:
	using AbstractConfiguration = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
	using Keys = std::vector<std::string>;

	void initializeStruct(const std::string priority,
						  const AbstractConfiguration & stopCriterionsView)
	{
		Keys keys;
		stopCriterionsView->keys(priority, keys);

		for (const std::string & key : keys) {
			if (key == "timeout_ms") {
				timeout_ms.value    = stopCriterionsView->getUInt64(priority + ".timeout_ms");
				timeout_ms.priority = priority;
			} else if (key == "read_rows") {
				read_rows.value    = stopCriterionsView->getUInt64(priority + ".read_rows");
				read_rows.priority = priority;
			} else if (key == "bytes_read_uncompressed") {
				bytes_read_uncompressed.value    = stopCriterionsView->getUInt64(priority + ".bytes_read_uncompressed");
				bytes_read_uncompressed.priority = priority;
			} else if (key == "iterations") {
				iterations.value    = stopCriterionsView->getUInt64(priority + ".iterations");
				iterations.priority = priority;
			} else if (key == "min_time_not_changing_for_ms") {
				min_time_not_changing_for_ms.value    = stopCriterionsView->getUInt64(priority + ".min_time_not_changing_for_ms");
				min_time_not_changing_for_ms.priority = priority;
			} else if (key == "max_speed_not_changing_for_ms") {
				max_speed_not_changing_for_ms.value    = stopCriterionsView->getUInt64(priority + ".max_speed_not_changing_for_ms");
				max_speed_not_changing_for_ms.priority = priority;
			} else if (key == "average_speed_not_changing_for_ms") {
				average_speed_not_changing_for_ms.value    = stopCriterionsView->getUInt64(priority + ".average_speed_not_changing_for_ms");
				average_speed_not_changing_for_ms.priority = priority;
			} else {
				throw Poco::Exception("Met unkown stop criterion: " + key, 1);
			}

			if (priority == "min") { ++number_of_initialized_min; };
			if (priority == "max") { ++number_of_initialized_max; };
		}
	}

public:
	StopCriterions()
		: number_of_initialized_min(0), number_of_initialized_max(0),
		  fulfilled_criterions_min(0), fulfilled_criterions_max(0) {}

	void loadFromConfig(const AbstractConfiguration & stopCriterionsView)
	{
		if (stopCriterionsView->has("min")) {
			initializeStruct("min", stopCriterionsView);
		}

		if (stopCriterionsView->has("max")) {
			initializeStruct("max", stopCriterionsView);
		}
	}

	struct criterionWithPriority timeout_ms;
	struct criterionWithPriority read_rows;
	struct criterionWithPriority bytes_read_uncompressed;
	struct criterionWithPriority iterations;
	struct criterionWithPriority min_time_not_changing_for_ms;
	struct criterionWithPriority max_speed_not_changing_for_ms;
	struct criterionWithPriority average_speed_not_changing_for_ms;

    /// Hereafter 'min' and 'max', in context of critetions, mean a level of importance
	/// Number of initialized properties met in configuration
	std::atomic<size_t> number_of_initialized_min;
	std::atomic<size_t> number_of_initialized_max;

	std::atomic<size_t> fulfilled_criterions_min;
	std::atomic<size_t> fulfilled_criterions_max;
};

struct Stats
{
	Stopwatch watch;
	Stopwatch min_time_watch;
	Stopwatch max_speed_watch;
	Stopwatch average_speed_watch;
	// size_t queries; TODO: Do I need this?
	size_t read_rows;
	size_t read_bytes;

	/// min_time in ms
	UInt64 min_time      = std::numeric_limits<UInt64>::max();
	size_t max_speed     = 0;
	size_t average_speed = 0;
	size_t number_of_speed_info_batches = 0;

	void update_min_time(const UInt64 min_time_candidate)
	{
		// TODO:
		std::cout << "current min_time: " << min_time << std::endl;
		std::cout << "min_time candidate: " << min_time_candidate << std::endl;
		std::cout << std::endl;

		if (min_time_candidate < min_time) {
			min_time = min_time_candidate;
			min_time_watch.restart();
		}
	}

	void update_average_speed(const size_t new_speed_info)
	{
		size_t new_average_speed = ((average_speed * number_of_speed_info_batches)
									+ new_speed_info);
		new_average_speed /= (++number_of_speed_info_batches);
		if (new_average_speed != average_speed) {
			average_speed = new_average_speed;
			average_speed_watch.restart();
		}
	}

	void update_max_speed(const size_t max_speed_candidate)
	{
		if (max_speed_candidate > max_speed) {
			max_speed = max_speed_candidate;
			max_speed_watch.restart();
		}
	}

	void add(size_t read_rows_inc, size_t read_bytes_inc)
	{
		read_rows  += read_rows_inc;
		read_bytes += read_bytes_inc;

		size_t new_speed = read_rows_inc / watch.elapsedSeconds();
		update_max_speed(new_speed);
		update_average_speed(new_speed);
	}

	void clear()
	{
		watch.restart();

		read_rows  = 0;
		read_bytes = 0;
	}
};

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
        const std::vector<std::string> & without_names_regexp
	):
	   concurrency(concurrency_), queue(concurrency_),
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

	using Query   = std::string;
	using Queries = std::vector<std::string>;
	Queries queries;

	using Queue = ConcurrentBoundedQueue<Query>;
	Queue queue;

	ConnectionPool connections;
	ThreadPool     pool;
	Settings       settings;

	using XMLConfiguration  = Poco::Util::XMLConfiguration;
	using AbstractConfig    = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
	using Config            = Poco::AutoPtr<XMLConfiguration>;
	using Paths             = std::vector<std::string>;
	using StringToVector    = std::map< std::string, std::vector<std::string> >;
	std::vector<Config> testsConfigurations;

	struct StopCriterions stopCriterions;

	#define incFulfilledCriterions(CRITERION) \
		stopCriterions.CRITERION.priority == "min" \
				? ++stopCriterions.fulfilled_criterions_min \
				: ++stopCriterions.fulfilled_criterions_max;

	enum ExecutionType { loop, once };
	ExecutionType execType;

	Stats info_total;
	std::mutex mutex;


	void readTestsConfiguration(const Paths & input_files)
	{
		testsConfigurations.resize(input_files.size());

		for (size_t i = 0; i != input_files.size(); ++i) {
			const std::string path = input_files[i];
			testsConfigurations[i] = Config(new XMLConfiguration(path));
		}

		// TODO: here will be tests filter on tags, names, regexp matching, etc.
		// { ... }

		// for now let's launch one test only
		if (testsConfigurations.size()) {
			for (auto & testConfig : testsConfigurations) {
				runTest(testConfig);
			}
		}
	}

	void runTest(Config & testConfig)
	{
		std::string testName = testConfig->getString("name");
		std::cout << "Running: " << testName << "\n";

		/// Preprocess configuration file
		using Keys = std::vector<std::string>;

		if (testConfig->has("settings")) {
			Keys configSettings;
			testConfig->keys("settings", configSettings);

			/// This macro goes through all settings in the Settings.h
			/// and, if found any settings in test's xml configuration
			/// with the same name, sets its value to settings
			std::vector<std::string>::iterator it;
			#define EXTRACT_SETTING(TYPE, NAME, DEFAULT) \
				it = std::find(configSettings.begin(), configSettings.end(), #NAME); \
				if (it != configSettings.end()) \
					settings.set( \
						#NAME, testConfig->getString("settings."#NAME) \
					);
				APPLY_FOR_SETTINGS(EXTRACT_SETTING)
				APPLY_FOR_LIMITS(EXTRACT_SETTING)
			#undef EXTRACT_SETTING

			if (std::find(configSettings.begin(), configSettings.end(), "profile") !=
				configSettings.end()) {
				// TODO: proceed profile settings in a proper way
			}
		}

		Query query;

		if (! testConfig->has("query")) {
			throw Poco::Exception("Missing query field in test's config: " +
								  testName, 1);
		}

		query = testConfig->getString("query");

		if (query.empty()) {
			throw Poco::Exception("The query is empty in test's config: " +
								  testName, 1);
		}

		if (testConfig->has("substitutions")) {
			/// Make "subconfig" of inner xml block
			AbstractConfig substitutionsView(testConfig
											 ->createView("substitutions"));

			StringToVector substitutions;
			constructSubstitutions(substitutionsView, substitutions);

			queries = formatQueries(query, substitutions);
		} else {
			// TODO: probably it will be a good practice to check if
			// query string has {substitution pattern}, but no substitution field
			// was found in xml configuration

			queries.push_back(query);
		}

		if (! testConfig->has("type")) {
			throw Poco::Exception("Missing type property in config: " +
								  testName);
		}

		std::string configExecType = testConfig->getString("type");
		if (configExecType == "loop")
			execType = loop;
		else if (configExecType == "once")
			execType = once;
		else
			throw Poco::Exception("Unknown type " + configExecType + " in :" +
								  testName, 1);

		if (testConfig->has("stop")) {
			AbstractConfig stopCriterionsView(testConfig
										      ->createView("stop"));
			stopCriterions.loadFromConfig(stopCriterionsView);
		} else {
			throw Poco::Exception("No termination conditions were found", 1);
		}

		if (execType == loop) {
			runLoopQuery(queries[0]);
		} else {
			runQueries(queries);
		}

	}

	void runLoopQuery(const Query & query)
	{
		info_total.watch.restart();
		info_total.min_time_watch.restart();

		size_t max_iterations = stopCriterions.iterations.value;
		size_t i = -1;

		while (true) {
			++i;

			pool.schedule(std::bind(
				&PerformanceTest::thread,
				this,
				connections.IConnectionPool::get()
			));

			queue.push(query);
			queue.push(""); /// asking thread to stop
			pool.wait();

			/// check stop criterions
			if (max_iterations && i >= max_iterations) {
				incFulfilledCriterions(iterations);
			}

			if (stopCriterions.number_of_initialized_min &&
				(stopCriterions.fulfilled_criterions_min >=
				stopCriterions.number_of_initialized_min)) {
				/// All 'min' criterions are fulfilled
				// TODO:
				std::cout << "All 'min' criterions are fulfilled" << std::endl;
				break;
			}

			if (stopCriterions.number_of_initialized_max &&
				stopCriterions.fulfilled_criterions_max) {
				/// Some 'max' criterions are fulfilled

				// TODO:
				std::cout << stopCriterions.fulfilled_criterions_max
						  << "'max' criterions are fulfilled" << std::endl;
				break;
			}
		}
	}

	void runQueries(const Queries & queries)
	{
		info_total.watch.restart();
		info_total.min_time_watch.restart();

		for (size_t i = 0; i < concurrency; ++i) {
			pool.schedule(std::bind(
				&PerformanceTest::thread,
				this,
				connections.IConnectionPool::get()
			));
		}

		for (const Query & query : queries) {
			queue.push(query);
		}

		for (size_t i = 0; i != concurrency; ++i) {
			/// Genlty asking threads to stop
			queue.push("");
		}

		pool.wait();
	}

	void thread(ConnectionPool::Entry & connection)
	{
		Stats info_per_query;

		Query query;

		while (true) {
			queue.pop(query);

			/// Empty query means end of execution
			if (query.empty())
				break;

			execute(connection, query, info_per_query);
		}
	}

	void execute(ConnectionPool::Entry & connection, const Query & query,
													 Stats & info_per_query)
	{
		// TODO:
		std::cout << "execute? " << query << std::endl;

		RemoteBlockInputStream stream(connection, query, &settings, nullptr,
									  Tables()/*, query_processing_stage*/);

		Progress progress;
		stream.setProgressCallback(
			[&progress, &stream, &info_per_query, this]
			(const Progress & value) {
				// TODO:
				std::cout << "got some progress" << std::endl;

				progress.incrementPiecewiseAtomically(value);

				this->checkFulfilledCriterionsAndUpdate(progress, stream, info_per_query);
		});

		info_per_query.watch.restart();
		info_per_query.average_speed_watch.restart();
		info_per_query.max_speed_watch.restart();

		stream.readPrefix();
		while (Block block = stream.read())
			;
		stream.readSuffix();

													// cast nanoseconds to ms
		UInt64 queryExecutionTime = info_per_query.min_time_watch.elapsed()
																/ (1000 * 1000);
		info_total.update_min_time(queryExecutionTime);

		// const BlockStreamProfileInfo & info = stream.getProfileInfo();

		// double seconds = watch.elapsedSeconds();

		// std::lock_guard<std::mutex> lock(mutex);
		// info_per_interval.add(seconds, progress.rows, progress.bytes, info.rows, info.bytes);
		// info_total.add(seconds, progress.rows, progress.bytes, info.rows, info.bytes);
	}

	void checkFulfilledCriterionsAndUpdate(const Progress & progress,
										   RemoteBlockInputStream & stream,
										   Stats & info_per_query)
	{
		// TODO:
		std::cout << "im checking" << std::endl;

		std::lock_guard<std::mutex> lock(mutex);

		info_total.add(progress.rows, progress.bytes);
		info_per_query.add(progress.rows, progress.bytes);

		size_t max_rows_to_read = stopCriterions.read_rows.value;
		if (max_rows_to_read && info_total.read_rows >= max_rows_to_read) {
			incFulfilledCriterions(read_rows);
		}

		size_t max_bytes_to_read = stopCriterions.bytes_read_uncompressed.value;
		if (max_bytes_to_read && info_total.read_bytes >= max_bytes_to_read) {
			incFulfilledCriterions(bytes_read_uncompressed);
		}

		if (UInt64 max_timeout_ms = stopCriterions.timeout_ms.value) {
			/// cast nanoseconds to ms
			if ((info_total.watch.elapsed() / (1000 * 1000)) > max_timeout_ms) {
				incFulfilledCriterions(timeout_ms);
			}
		}

		size_t min_time_not_changing_for_ms = stopCriterions
												.min_time_not_changing_for_ms.value;
		if (min_time_not_changing_for_ms) {
			// TODO:
			std::cout << "min time is in attention" << std::endl;

			size_t min_time_did_not_change_for = info_total
													.min_time_watch
													.elapsed() / (1000 * 1000);
			// TODO:
			std::cout << "min_time_did_not_change_for: " << min_time_did_not_change_for << std::endl;
			std::cout << "min_time_not_changing_for_ms: " << min_time_not_changing_for_ms << std::endl;

			if (min_time_did_not_change_for >= min_time_not_changing_for_ms) {
				// TODO:
				std::cout << "min time yeahh" << std::endl;
				incFulfilledCriterions(min_time_not_changing_for_ms);
			}
		}

		size_t max_speed_not_changing_for_ms = stopCriterions
												.max_speed_not_changing_for_ms
												.value;
		if (max_speed_not_changing_for_ms) {
			UInt64 speed_not_changing_time = info_per_query
												.max_speed_watch
												.elapsed() / (1000 * 1000);
			if (speed_not_changing_time >= max_speed_not_changing_for_ms) {
				incFulfilledCriterions(max_speed_not_changing_for_ms);
			}
		}

		size_t average_speed_not_changing_for_ms = stopCriterions
													.average_speed_not_changing_for_ms
													.value;
		if (average_speed_not_changing_for_ms) {
			UInt64 speed_not_changing_time = info_per_query
												.average_speed_watch
												.elapsed() / (1000 * 1000);
			if (speed_not_changing_time >= average_speed_not_changing_for_ms) {
				incFulfilledCriterions(average_speed_not_changing_for_ms);
			}
		}

		if (stopCriterions.number_of_initialized_min &&
			(stopCriterions.fulfilled_criterions_min >=
			stopCriterions.number_of_initialized_min)) {
			/// All 'min' criterions are fulfilled
			// TODO:
			std::cout << "All 'min' criterions are fulfilled" << std::endl;
			stream.cancel();
		}

		if (stopCriterions.number_of_initialized_max &&
			stopCriterions.fulfilled_criterions_max) {
			/// Some 'max' criterions are fulfilled

			// TODO:
			std::cout << stopCriterions.fulfilled_criterions_max
					  << "'max' criterions are fulfilled" << std::endl;
			stream.cancel();
		}
	}

	void constructSubstitutions(AbstractConfig & substitutionsView,
						        StringToVector & substitutions)
	{
		using Keys = std::vector<std::string>;
		Keys xml_substitutions;
		substitutionsView->keys(xml_substitutions);

		for (size_t i = 0; i != xml_substitutions.size(); ++i) {
			const AbstractConfig xml_substitution(
				substitutionsView->createView("substitution[" +
			   								  std::to_string(i) + "]")
			);

			/// Property values for substitution will be stored in a vector
			/// accessible by property name
			std::vector<std::string> xml_values;
			xml_substitution->keys("values", xml_values);

			std::string name = xml_substitution->getString("name");

			for (size_t j = 0; j != xml_values.size(); ++j) {
				substitutions[name].push_back(
					xml_substitution->getString("values.value[" +
											    std::to_string(j) + "]")
				);
			}
		}
	}

	std::vector<std::string> formatQueries(const std::string & query,
					   					   StringToVector substitutions) const
	{
		std::vector<std::string> queries;

		StringToVector::iterator substitutions_first = substitutions.begin();
		StringToVector::iterator substitutions_last  = substitutions.end();
		--substitutions_last;

		runThroughAllOptionsAndPush(
			substitutions_first, substitutions_last, query, queries
		);

		return queries;
	}

	/// Recursive method which goes through all substitution blocks in xml
	/// and replaces property {names} by their values
	void runThroughAllOptionsAndPush(
		StringToVector::iterator substitutions_left,
		StringToVector::iterator substitutions_right,
		const std::string & template_query,
		std::vector<std::string> & queries
	) const
	{
		std::string name = substitutions_left->first;
		std::vector<std::string> values = substitutions_left->second;

		for (auto value = values.begin(); value != values.end(); ++value) {
			/// Copy query string for each unique permutation
			Query query = template_query;
			size_t substrPos  = 0;

			while (substrPos != std::string::npos) {
				substrPos = query.find("{" + name + "}");

				if (substrPos != std::string::npos) {
					query.replace(
						substrPos, 1 + name.length() + 1,
						*value
					);
				}
			}

			/// If we've reached the end of substitution chain
			if (substitutions_left == substitutions_right) {
				queries.push_back(query);
			} else {
				StringToVector::iterator next_it = substitutions_left;
				++next_it;

				runThroughAllOptionsAndPush(
					next_it, substitutions_right, query, queries
				);
			}
		}
	}
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
			skip_matching_regexp
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
