#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>

#include <iostream>
#include <fstream>
#include <iomanip>

#include <Poco/File.h>
#include <Poco/SharedPtr.h>
#include <Poco/Util/Application.h>

#include <statdaemons/Stopwatch.h>
#include <statdaemons/threadpool.hpp>
#include <stats/ReservoirSampler.h>

#include <boost/program_options.hpp>

#include <DB/Common/ConcurrentBoundedQueue.h>

#include <DB/Core/Exception.h>
#include <DB/Core/Types.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/copyData.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>

#include <DB/Interpreters/Context.h>

#include <DB/Client/Connection.h>

#include "InterruptListener.h"


/** Инструмент для измерения производительности ClickHouse
  *  при выполнении запросов с фиксированным количеством одновременных запросов.
  */

namespace DB
{

class Benchmark
{
public:
	Benchmark(unsigned concurrency_, double delay_,
			const String & host_, UInt16 port_, const String & default_database_,
			const String & user_, const String & password_, const Settings & settings_)
		: concurrency(concurrency_), delay(delay_), queue(concurrency),
		connections(concurrency, host_, port_, default_database_, user_, password_, data_type_factory),
		settings(settings_), pool(concurrency)
	{
		std::cerr << std::fixed << std::setprecision(3);

		readQueries();
		run();
	}

private:
	typedef std::string Query;

	unsigned concurrency;
	double delay;

	typedef std::vector<Query> Queries;
	Queries queries;

	typedef ConcurrentBoundedQueue<Query> Queue;
	Queue queue;

	DataTypeFactory data_type_factory;
	ConnectionPool connections;
	Settings settings;

	struct Stats
	{
		Stopwatch watch;
		size_t queries = 0;
		size_t read_rows = 0;
		size_t read_bytes = 0;
		size_t result_rows = 0;
		size_t result_bytes = 0;

		typedef ReservoirSampler<double> Sampler;
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

	Poco::FastMutex mutex;

	boost::threadpool::pool pool;


	void readQueries()
	{
		ReadBufferFromFileDescriptor in(STDIN_FILENO);

		while (!in.eof())
		{
			std::string query;
			readText(query, in);
			assertString("\n", in);

			if (!query.empty())
				queries.emplace_back(query);
		}

		if (queries.empty())
			throw Exception("Empty list of queries.");

		std::cerr << "Loaded " << queries.size() << " queries." << std::endl;
	}


	void run()
	{
		for (size_t i = 0; i < concurrency; ++i)
			pool.schedule(std::bind(&Benchmark::thread, this, connections.get()));

		InterruptListener interrupt_listener;

		info_per_interval.watch.restart();
		Stopwatch watch;

		/// В цикле, кладём все запросы в очередь.
		for (size_t i = 0; !interrupt_listener.check(); ++i)
		{
			if (i >= queries.size())
				i = 0;

			queue.push(queries[i]);

			if (watch.elapsedSeconds() > delay)
			{
				report(info_per_interval);
				watch.restart();
			}
		}

		/// Попросим потоки завершиться.
		for (size_t i = 0; i < concurrency; ++i)
			queue.push("");

		pool.wait();

		std::cerr << "\nTotal queries executed: " << info_total.queries << std::endl;
		report(info_total);
	}


	void thread(ConnectionPool::Entry connection)
	{
		try
		{
			/// В этих потоках не будем принимать сигнал INT.
			sigset_t sig_set;
			if (sigemptyset(&sig_set)
				|| sigaddset(&sig_set, SIGINT)
				|| pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
				throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

			Query query;

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

			std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

			/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
			if (std::string::npos == text.find("Stack trace"))
				std::cerr << "Stack trace:" << std::endl
					<< e.getStackTrace().toString();

			throw;
		}
		catch (const Poco::Exception & e)
		{
			std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
			throw;
		}
		catch (const std::exception & e)
		{
			std::cerr << "std::exception: " << e.what() << std::endl;
			throw;
		}
		catch (...)
		{
			std::cerr << "Unknown exception" << std::endl;
			throw;
		}
	}


	void execute(ConnectionPool::Entry & connection, Query & query)
	{
		Stopwatch watch;
		RemoteBlockInputStream stream(connection, query, &settings);

		Progress progress;
		stream.setProgressCallback([&progress](const Progress & value) { progress.incrementPiecewiseAtomically(value); });

		stream.readPrefix();
		while (Block block = stream.read())
			;
		stream.readSuffix();

		const BlockStreamProfileInfo & info = stream.getInfo();

		double seconds = watch.elapsedSeconds();

		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		info_per_interval.add(seconds, progress.rows, progress.bytes, info.rows, info.bytes);
		info_total.add(seconds, progress.rows, progress.bytes, info.rows, info.bytes);
	}


	void report(Stats & info)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		double seconds = info.watch.elapsedSeconds();

		std::cerr
			<< std::endl
			<< "QPS: " << (info.queries / seconds) << ", "
			<< "RPS: " << (info.read_rows / seconds) << ", "
			<< "MiB/s: " << (info.read_bytes / seconds / 1048576) << ", "
			<< "result RPS: " << (info.result_rows / seconds) << ", "
			<< "result MiB/s: " << (info.result_bytes / seconds / 1048576) << "."
			<< std::endl;

		for (size_t percent = 0; percent <= 90; percent += 10)
			std::cerr << percent << "%\t" << info.sampler.quantileInterpolated(percent / 100.0) << " sec." << std::endl;

		std::cerr << "95%\t" 	<< info.sampler.quantileInterpolated(0.95) 	<< " sec." << std::endl;
		std::cerr << "99%\t" 	<< info.sampler.quantileInterpolated(0.99) 	<< " sec." << std::endl;
		std::cerr << "99.9%\t" 	<< info.sampler.quantileInterpolated(0.999) 	<< " sec." << std::endl;
		std::cerr << "99.99%\t" << info.sampler.quantileInterpolated(0.9999) << " sec." << std::endl;
		std::cerr << "100%\t" 	<< info.sampler.quantileInterpolated(1) 		<< " sec." << std::endl;

		info.clear();
	}
};

}


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		boost::program_options::options_description desc("Allowed options");
		desc.add_options()
			("help", "produce help message")
			("concurrency,c", boost::program_options::value<unsigned>()->default_value(1), "number of parallel queries")
			("delay,d", boost::program_options::value<double>()->default_value(1), "delay between reports in seconds")
			("host,h", boost::program_options::value<std::string>()->default_value("localhost"), "")
			("port", boost::program_options::value<UInt16>()->default_value(9000), "")
			("user", boost::program_options::value<std::string>()->default_value("default"), "")
			("password", boost::program_options::value<std::string>()->default_value(""), "")
			("database", boost::program_options::value<std::string>()->default_value("default"), "")
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
			std::cout << "Usage: " << argv[0] << " [options] < queries.txt" << std::endl;
			std::cout << desc << std::endl;
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
			settings);
	}
	catch (const Exception & e)
	{
		std::string text = e.displayText();

		std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

		/// Если есть стек-трейс на сервере, то не будем писать стек-трейс на клиенте.
		if (std::string::npos == text.find("Stack trace"))
			std::cerr << "Stack trace:" << std::endl
				<< e.getStackTrace().toString();

		return e.code();
	}
	catch (const Poco::Exception & e)
	{
		std::cerr << "Poco::Exception: " << e.displayText() << std::endl;
		return ErrorCodes::POCO_EXCEPTION;
	}
	catch (const std::exception & e)
	{
		std::cerr << "std::exception: " << e.what() << std::endl;
		return ErrorCodes::STD_EXCEPTION;
	}
	catch (...)
	{
		std::cerr << "Unknown exception" << std::endl;
		return ErrorCodes::UNKNOWN_EXCEPTION;
	}
}
