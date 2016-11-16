#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>

#include <iostream>
#include <fstream>
#include <iomanip>
#include <random>

#include <Poco/File.h>
#include <Poco/Util/Application.h>

#include <DB/Common/Stopwatch.h>
#include <DB/Common/ThreadPool.h>
#include <DB/AggregateFunctions/ReservoirSampler.h>

#include <boost/program_options.hpp>

#include <DB/Common/ConcurrentBoundedQueue.h>

#include <DB/Common/Exception.h>
#include <DB/Common/randomSeed.h>
#include <DB/Core/Types.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/Operators.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Interpreters/Context.h>

#include <DB/Client/Connection.h>

#include "InterruptListener.h"


/** A tool for evaluating ClickHouse performance.
  * The tool emulates a case with fixed amount of simultaneously executing queries.
  */

namespace DB
{

namespace ErrorCodes
{
	extern const int POCO_EXCEPTION;
	extern const int STD_EXCEPTION;
	extern const int UNKNOWN_EXCEPTION;
}

class Benchmark
{
public:
	Benchmark(unsigned concurrency_, double delay_,
			const String & host_, UInt16 port_, const String & default_database_,
			const String & user_, const String & password_, const String & stage,
			bool randomize_, size_t max_iterations_, double max_time_,
			const String & json_path_, const Settings & settings_)
		:
		concurrency(concurrency_), delay(delay_), queue(concurrency),
		connections(concurrency, host_, port_, default_database_, user_, password_),
		randomize(randomize_), max_iterations(max_iterations_), max_time(max_time_),
		json_path(json_path_), settings(settings_), pool(concurrency)
	{
		std::cerr << std::fixed << std::setprecision(3);

		if (stage == "complete")
			query_processing_stage = QueryProcessingStage::Complete;
		else if (stage == "fetch_columns")
			query_processing_stage = QueryProcessingStage::FetchColumns;
		else if (stage == "with_mergeable_state")
			query_processing_stage = QueryProcessingStage::WithMergeableState;
		else
			throw Exception("Unknown query processing stage: " + stage, ErrorCodes::BAD_ARGUMENTS);

		if (!json_path.empty() && Poco::File(json_path).exists()) /// Clear file with previous results
		{
			Poco::File(json_path).remove();
		}

		readQueries();
		run();
	}

private:
	using Query = std::string;

	unsigned concurrency;
	double delay;

	using Queries = std::vector<Query>;
	Queries queries;

	using Queue = ConcurrentBoundedQueue<Query>;
	Queue queue;

	ConnectionPool connections;
	bool randomize;
	size_t max_iterations;
	double max_time;
	String json_path;
	Settings settings;
	QueryProcessingStage::Enum query_processing_stage;

	struct Stats
	{
		Stopwatch watch;
		size_t queries = 0;
		size_t read_rows = 0;
		size_t read_bytes = 0;
		size_t result_rows = 0;
		size_t result_bytes = 0;

		using Sampler = ReservoirSampler<double>;
		Sampler sampler {1 << 16};

		void add(double seconds, size_t read_rows_inc, size_t read_bytes_inc, size_t result_rows_inc, size_t result_bytes_inc)
		{
			++queries;
			read_rows += read_rows_inc;
			read_bytes += read_bytes_inc;
			result_rows += result_rows_inc;
			result_bytes += result_bytes_inc;
			sampler.insert(seconds);
		}

		void clear()
		{
			watch.restart();
			queries = 0;
			read_rows = 0;
			read_bytes = 0;
			result_rows = 0;
			result_bytes = 0;
			sampler.clear();
		}
	};

	Stats info_per_interval;
	Stats info_total;

	std::mutex mutex;

	ThreadPool pool;


	void readQueries()
	{
		ReadBufferFromFileDescriptor in(STDIN_FILENO);

		while (!in.eof())
		{
			std::string query;
			readText(query, in);
			assertChar('\n', in);

			if (!query.empty())
				queries.emplace_back(query);
		}

		if (queries.empty())
			throw Exception("Empty list of queries.");

		std::cerr << "Loaded " << queries.size() << " queries.\n";
	}


	void printNumberOfQueriesExecuted(size_t num)
	{
		std::cerr << "\nQueries executed: " << num;
		if (queries.size() > 1)
			std::cerr << " (" << (num * 100.0 / queries.size()) << "%)";
		std::cerr << ".\n";
	}


	void run()
	{
		std::mt19937 generator(randomSeed());
		std::uniform_int_distribution<size_t> distribution(0, queries.size() - 1);

		for (size_t i = 0; i < concurrency; ++i)
			pool.schedule(std::bind(&Benchmark::thread, this, connections.IConnectionPool::get()));

		InterruptListener interrupt_listener;

		info_per_interval.watch.restart();
		Stopwatch watch;

		/// В цикле, кладём все запросы в очередь.
		for (size_t i = 0; !max_iterations || i < max_iterations; ++i)
		{
			size_t query_index = randomize ? distribution(generator) : i % queries.size();

			queue.push(queries[query_index]);

			if (delay > 0 && watch.elapsedSeconds() > delay)
			{
				auto total_queries = 0;
				{
					std::lock_guard<std::mutex> lock(mutex);
					total_queries = info_total.queries;
				}
				printNumberOfQueriesExecuted(total_queries);

				report(info_per_interval);
				watch.restart();
			}

			if (max_time > 0 && info_total.watch.elapsedSeconds() >= max_time)
			{
				std::cout << "Stopping launch of queries. Requested time limit is exhausted.\n";
				break;
			}

			if (interrupt_listener.check())
			{
				std::cout << "Stopping launch of queries. SIGINT recieved.\n";
				break;
			}
		}

		/// Попросим потоки завершиться.
		for (size_t i = 0; i < concurrency; ++i)
			queue.push("");

		pool.wait();

		info_total.watch.stop();
		if (!json_path.empty())
			reportJSON(info_total, json_path);
		printNumberOfQueriesExecuted(info_total.queries);
		report(info_total);
	}


	void thread(ConnectionPool::Entry connection)
	{
		Query query;

		try
		{
			try
			{
				/// В этих потоках не будем принимать сигнал INT.
				sigset_t sig_set;
				if (sigemptyset(&sig_set)
					|| sigaddset(&sig_set, SIGINT)
					|| pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
					throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

				while (true)
				{
					queue.pop(query);

					/// Пустой запрос обозначает конец работы.
					if (query.empty())
						break;

					execute(connection, query);
				}
			}
			catch (const Exception & e)
			{
				std::string text = e.displayText();

				std::cerr << "Code: " << e.code() << ". " << text << "\n\n";

				/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
				if (std::string::npos == text.find("Stack trace"))
					std::cerr << "Stack trace:\n"
						<< e.getStackTrace().toString();

				throw;
			}
			catch (const Poco::Exception & e)
			{
				std::cerr << "Poco::Exception: " << e.displayText() << "\n";
				throw;
			}
			catch (const std::exception & e)
			{
				std::cerr << "std::exception: " << e.what() << "\n";
				throw;
			}
			catch (...)
			{
				std::cerr << "Unknown exception\n";
				throw;
			}
		}
		catch (...)
		{
			std::cerr << "On query:\n" << query << "\n";
			throw;
		}
	}


	void execute(ConnectionPool::Entry & connection, Query & query)
	{
		Stopwatch watch;
		RemoteBlockInputStream stream(connection, query, &settings, nullptr, Tables(), query_processing_stage);

		Progress progress;
		stream.setProgressCallback([&progress](const Progress & value) { progress.incrementPiecewiseAtomically(value); });

		stream.readPrefix();
		while (Block block = stream.read())
			;
		stream.readSuffix();

		const BlockStreamProfileInfo & info = stream.getProfileInfo();

		double seconds = watch.elapsedSeconds();

		std::lock_guard<std::mutex> lock(mutex);
		info_per_interval.add(seconds, progress.rows, progress.bytes, info.rows, info.bytes);
		info_total.add(seconds, progress.rows, progress.bytes, info.rows, info.bytes);
	}


	void report(Stats & info)
	{
		std::lock_guard<std::mutex> lock(mutex);

		double seconds = info.watch.elapsedSeconds();

		std::cerr
			<< "\n"
			<< "QPS: " << (info.queries / seconds) << ", "
			<< "RPS: " << (info.read_rows / seconds) << ", "
			<< "MiB/s: " << (info.read_bytes / seconds / 1048576) << ", "
			<< "result RPS: " << (info.result_rows / seconds) << ", "
			<< "result MiB/s: " << (info.result_bytes / seconds / 1048576) << "."
			<< "\n";

		auto print_percentile = [&](double percent)
		{
			std::cerr << percent << "%\t" << info.sampler.quantileInterpolated(percent / 100.0) << " sec." << std::endl;
		};

		for (int percent = 0; percent <= 90; percent += 10)
			print_percentile(percent);

		print_percentile(95);
		print_percentile(99);
		print_percentile(99.9);
		print_percentile(99.99);

		info.clear();
	}

	void reportJSON(Stats & info, const std::string & filename)
	{
		WriteBufferFromFile json_out(filename);

		std::lock_guard<std::mutex> lock(mutex);

		auto print_key_value = [&](auto key, auto value, bool with_comma = true)
		{
			json_out << double_quote << key << ": " << value << (with_comma ? ",\n" : "\n");
		};

		auto print_percentile = [&](auto percent, bool with_comma = true)
		{
			json_out << "\"" << percent << "\"" << ": " << info.sampler.quantileInterpolated(percent / 100.0) << (with_comma ? ",\n" : "\n");
		};

		json_out << "{\n";

		json_out << double_quote << "statistics" << ": {\n";

		double seconds = info.watch.elapsedSeconds();
		print_key_value("QPS", info.queries / seconds);
		print_key_value("RPS", info.queries / seconds);
		print_key_value("MiBPS", info.queries / seconds);
		print_key_value("RPS_result", info.queries / seconds);
		print_key_value("MiBPS_result", info.queries / seconds);
		print_key_value("num_queries", info.queries / seconds, false);

		json_out << "},\n";


		json_out << double_quote << "query_time_percentiles" << ": {\n";

		for (int percent = 0; percent <= 90; percent += 10)
			print_percentile(percent);

		print_percentile(95);
		print_percentile(99);
		print_percentile(99.9);
		print_percentile(99.99, false);

		json_out << "}\n";

		json_out << "}\n";
	}
};

}


int mainEntryClickhouseBenchmark(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		using boost::program_options::value;

		boost::program_options::options_description desc("Allowed options");
		desc.add_options()
			("help", 																"produce help message")
			("concurrency,c",	value<unsigned>()->default_value(1), 				"number of parallel queries")
			("delay,d", 		value<double>()->default_value(1), 					"delay between intermediate reports in seconds (set 0 to disable reports)")
			("stage",			value<std::string>()->default_value("complete"), 	"request query processing up to specified stage")
			("iterations,i",	value<size_t>()->default_value(0),					"amount of queries to be executed")
			("timelimit,t",		value<double>()->default_value(0.), 				"stop launch of queries after specified time limit")
			("randomize,r",		value<bool>()->default_value(false),				"randomize order of execution")
			("json",			value<std::string>()->default_value(""),			"write final report to specified file in JSON format")
			("host,h",			value<std::string>()->default_value("localhost"), 	"")
			("port", 			value<UInt16>()->default_value(9000), 				"")
			("user", 			value<std::string>()->default_value("default"),		"")
			("password",		value<std::string>()->default_value(""), 			"")
			("database",		value<std::string>()->default_value("default"), 	"")

		#define DECLARE_SETTING(TYPE, NAME, DEFAULT) (#NAME, boost::program_options::value<std::string> (), "Settings.h")
		#define DECLARE_LIMIT(TYPE, NAME, DEFAULT) (#NAME, boost::program_options::value<std::string> (), "Limits.h")
			APPLY_FOR_SETTINGS(DECLARE_SETTING)
			APPLY_FOR_LIMITS(DECLARE_LIMIT)
		#undef DECLARE_SETTING
		#undef DECLARE_LIMIT
		;

		boost::program_options::variables_map options;
		boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

		if (options.count("help"))
		{
			std::cout << "Usage: " << argv[0] << " [options] < queries.txt\n";
			std::cout << desc << "\n";
			return 1;
		}

		/// Извлекаем settings and limits из полученных options
		Settings settings;

		#define EXTRACT_SETTING(TYPE, NAME, DEFAULT) \
		if (options.count(#NAME)) \
			settings.set(#NAME, options[#NAME].as<std::string>());
		APPLY_FOR_SETTINGS(EXTRACT_SETTING)
		APPLY_FOR_LIMITS(EXTRACT_SETTING)
		#undef EXTRACT_SETTING

		Benchmark benchmark(
			options["concurrency"].as<unsigned>(),
			options["delay"].as<double>(),
			options["host"].as<std::string>(),
			options["port"].as<UInt16>(),
			options["database"].as<std::string>(),
			options["user"].as<std::string>(),
			options["password"].as<std::string>(),
			options["stage"].as<std::string>(),
			options["randomize"].as<bool>(),
			options["iterations"].as<size_t>(),
			options["timelimit"].as<double>(),
			options["json"].as<std::string>(),
			settings);
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
