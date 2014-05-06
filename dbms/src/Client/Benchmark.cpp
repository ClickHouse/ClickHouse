#define DBMS_CLIENT 1	/// Используется в Context.h

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

#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/formatAST.h>

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
			const String & user_, const String & password_)
		: concurrency(concurrency_), delay(delay_), queue(concurrency), pool(concurrency),
		connections(concurrency, host_, port_, default_database_, user_, password_, data_type_factory)
	{
		std::cerr << std::fixed << std::setprecision(3);

		readQueries();
		run();

		std::cerr << "\nTotal queries executed: " << queries_total << std::endl;
	}

private:
	typedef std::string Query;

	unsigned concurrency;
	double delay;

	typedef std::vector<Query> Queries;
	Queries queries;

	typedef ConcurrentBoundedQueue<Query> Queue;
	Queue queue;

	boost::threadpool::pool pool;

	DataTypeFactory data_type_factory;
	ConnectionPool connections;

	Stopwatch watch_per_interval;
	size_t queries_total = 0;
	size_t queries_per_interval = 0;
	size_t read_rows_per_interval = 0;
	size_t read_bytes_per_interval = 0;
	size_t result_rows_per_interval = 0;
	size_t result_bytes_per_interval = 0;

	typedef ReservoirSampler<double> Sampler;
	Sampler sampler {1 << 16};
	Sampler global_sampler {1 << 16};

	Poco::FastMutex mutex;


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

		watch_per_interval.restart();
		Stopwatch watch;

		/// В цикле, кладём все запросы в очередь.
		for (size_t i = 0; !interrupt_listener.check(); ++i)
		{
			if (i >= queries.size())
				i = 0;

			queue.push(queries[i]);

			if (watch.elapsedSeconds() > delay)
			{
				report();
				watch.restart();
			}
		}

		/// Попросим потоки завершиться.
		for (size_t i = 0; i < concurrency; ++i)
			queue.push("");

		pool.wait();

		std::cerr << std::endl << "Totals:" << std::endl;
		reportTimings(global_sampler);
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
		RemoteBlockInputStream stream(connection, query, nullptr);

		size_t read_rows = 0;
		size_t read_bytes = 0;
		stream.setProgressCallback([&](size_t rows_inc, size_t bytes_inc) { read_rows += rows_inc; read_bytes += bytes_inc; });

		stream.readPrefix();
		while (Block block = stream.read())
			;
		stream.readSuffix();

		const BlockStreamProfileInfo & info = stream.getInfo();

		addTiming(watch.elapsedSeconds(), read_rows, read_bytes, info.rows, info.bytes);
	}


	void addTiming(double seconds, size_t read_rows, size_t read_bytes, size_t result_rows, size_t result_bytes)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		++queries_total;
		++queries_per_interval;
		read_rows_per_interval += read_rows;
		read_bytes_per_interval += read_bytes;
		result_rows_per_interval += result_rows;
		result_bytes_per_interval += result_bytes;
		sampler.insert(seconds);
		global_sampler.insert(seconds);
	}


	void reportTimings(Sampler & sampler)
	{
		for (size_t percent = 0; percent < 90; percent += 10)
			std::cerr << percent << "%\t" << sampler.quantileInterpolated(percent / 100.0) << " sec." << std::endl;

		std::cerr << "95%\t" 	<< sampler.quantileInterpolated(0.95) 	<< " sec." << std::endl;
		std::cerr << "99%\t" 	<< sampler.quantileInterpolated(0.99) 	<< " sec." << std::endl;
		std::cerr << "99.9%\t" 	<< sampler.quantileInterpolated(0.999) 	<< " sec." << std::endl;
		std::cerr << "99.99%\t" << sampler.quantileInterpolated(0.9999) << " sec." << std::endl;
		std::cerr << "100%\t" 	<< sampler.quantileInterpolated(1) 		<< " sec." << std::endl;
	}


	void report()
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		double seconds = watch_per_interval.elapsedSeconds();

		std::cerr
			<< std::endl
			<< "QPS: " << (queries_per_interval / seconds) << ", "
			<< "RPS: " << (read_rows_per_interval / seconds) << ", "
			<< "MiB/s: " << (read_bytes_per_interval / seconds / 1048576) << ", "
			<< "result RPS: " << (result_rows_per_interval / seconds) << ", "
			<< "result MiB/s: " << (result_bytes_per_interval / seconds / 1048576) << "."
			<< std::endl;

		reportTimings(sampler);
		resetCounts();
	}

	void resetCounts()
	{
		sampler.clear();
		queries_per_interval = 0;
		read_rows_per_interval = 0;
		read_bytes_per_interval = 0;
		result_rows_per_interval = 0;
		result_bytes_per_interval = 0;
		watch_per_interval.restart();
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
		;

		boost::program_options::variables_map options;
		boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

		if (options.count("help"))
		{
			std::cout << "Usage: " << argv[0] << " [options] < queries.txt" << std::endl;
			std::cout << desc << std::endl;
			return 1;
		}

		Benchmark benchmark(
			options["concurrency"].as<unsigned>(),
			options["delay"].as<double>(),
			options["host"].as<std::string>(),
			options["port"].as<UInt16>(),
			options["database"].as<std::string>(),
			options["user"].as<std::string>(),
			options["password"].as<std::string>());
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
