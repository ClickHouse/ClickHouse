#pragma once

#include <DB/DataStreams/RemoteBlockOutputStream.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/IO/ReadBufferFromFile.h>

#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>

#include <Poco/DirectoryIterator.h>

#include <thread>
#include <mutex>
#include <condition_variable>


namespace DB
{

namespace ErrorCodes
{
	extern const int INCORRECT_FILE_NAME;
	extern const int CHECKSUM_DOESNT_MATCH;
	extern const int TOO_LARGE_SIZE_COMPRESSED;
}


namespace
{
	static constexpr const std::chrono::seconds max_sleep_time{30};
	static constexpr const std::chrono::minutes decrease_error_count_period{5};

	template <typename PoolFactory>
	ConnectionPools createPoolsForAddresses(const std::string & name, PoolFactory && factory)
	{
		ConnectionPools pools;

		for (auto it = boost::make_split_iterator(name, boost::first_finder(",")); it != decltype(it){}; ++it)
		{
			const auto address = boost::copy_range<std::string>(*it);

			const auto user_pw_end = strchr(address.data(), '@');
			const auto colon = strchr(address.data(), ':');
			if (!user_pw_end || !colon)
				throw Exception{
					"Shard address '" + address + "' does not match to 'user[:password]@host:port' pattern",
					ErrorCodes::INCORRECT_FILE_NAME
				};

			const auto has_pw = colon < user_pw_end;
			const auto host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
			if (!host_end)
				throw Exception{
					"Shard address '" + address + "' does not contain port",
					ErrorCodes::INCORRECT_FILE_NAME
				};

			const auto user = unescapeForFileName({address.data(), has_pw ? colon : user_pw_end});
			const auto password = has_pw ? unescapeForFileName({colon + 1, user_pw_end}) : std::string{};
			const auto host = unescapeForFileName({user_pw_end + 1, host_end});
			const auto port = parse<UInt16>(host_end + 1);

			pools.emplace_back(factory(host, port, user, password));
		}

		return pools;
	}
}

/** Implementation for StorageDistributed::DirectoryMonitor nested class.
 *  This type is not designed for standalone use. */
class StorageDistributed::DirectoryMonitor
{
public:
	DirectoryMonitor(StorageDistributed & storage, const std::string & name)
		: storage(storage), pool{createPool(name)}, path{storage.path + name + '/'}
		, default_sleep_time{storage.context.getSettingsRef().distributed_directory_monitor_sleep_time_ms.totalMilliseconds()}
		, sleep_time{default_sleep_time}
		, log{&Logger::get(getLoggerName())}
	{
	}

	~DirectoryMonitor()
	{
		{
			quit = true;
			std::lock_guard<std::mutex> lock{mutex};
		}
		cond.notify_one();
		thread.join();
	}

private:
	void run()
	{
		setThreadName("DistrDirMonitor");

		std::unique_lock<std::mutex> lock{mutex};

		const auto quit_requested = [this] { return quit; };

		while (!quit_requested())
		{
			auto do_sleep = true;

			try
			{
				do_sleep = !findFiles();
			}
			catch (...)
			{
				do_sleep = true;
				++error_count;
				sleep_time = std::min(
					std::chrono::milliseconds{std::int64_t(default_sleep_time.count() * std::exp2(error_count))},
					std::chrono::milliseconds{max_sleep_time});
				tryLogCurrentException(getLoggerName().data());
			};

			if (do_sleep)
				cond.wait_for(lock, sleep_time, quit_requested);

			const auto now = std::chrono::system_clock::now();
			if (now - last_decrease_time > decrease_error_count_period)
			{
				error_count /= 2;
				last_decrease_time = now;
			}
		}
	}

	ConnectionPoolPtr createPool(const std::string & name)
	{
		const auto pool_factory = [this, &name] (const std::string & host, const UInt16 port,
												 const std::string & user, const std::string & password) {
			return new ConnectionPool{
				1, host, port, "",
				user, password,
				storage.getName() + '_' + name};
		};

		auto pools = createPoolsForAddresses(name, pool_factory);

		return pools.size() == 1 ? pools.front() : new ConnectionPoolWithFailover(pools, LoadBalancing::RANDOM);
	}

	bool findFiles()
	{
		std::map<UInt64, std::string> files;

		Poco::DirectoryIterator end;
		for (Poco::DirectoryIterator it{path}; it != end; ++it)
		{
			const auto & file_path_str = it->path();
			Poco::Path file_path{file_path_str};

			if (!it->isDirectory() && 0 == strncmp(file_path.getExtension().data(), "bin", strlen("bin")))
				files[parse<UInt64>(file_path.getBaseName())] = file_path_str;
		}

		if (files.empty())
			return false;

		for (const auto & file : files)
		{
			if (quit)
				return true;

			processFile(file.second);
		}

		return true;
	}

	void processFile(const std::string & file_path)
	{
		LOG_TRACE(log, "Started processing `" << file_path << '`');
		auto connection = pool->get();

		try
		{
			CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

			ReadBufferFromFile in{file_path};

			std::string insert_query;
			readStringBinary(insert_query, in);

			RemoteBlockOutputStream remote{*connection, insert_query};

			remote.writePrefix();
			remote.writePrepared(in);
			remote.writeSuffix();
		}
		catch (const Exception & e)
		{
			const auto code = e.code();

			/// mark file as broken if necessary
			if (code == ErrorCodes::CHECKSUM_DOESNT_MATCH ||
				code == ErrorCodes::TOO_LARGE_SIZE_COMPRESSED ||
				code == ErrorCodes::CANNOT_READ_ALL_DATA)
			{
				const auto last_path_separator_pos = file_path.rfind('/');
				const auto & path = file_path.substr(0, last_path_separator_pos + 1);
				const auto & file_name = file_path.substr(last_path_separator_pos + 1);
				const auto & broken_path = path + "broken/";
				const auto & broken_file_path = broken_path + file_name;

				Poco::File{broken_path}.createDirectory();
				Poco::File{file_path}.renameTo(broken_file_path);

				LOG_ERROR(log, "Renamed `" << file_path << "` to `" << broken_file_path << '`');
			}

			throw;
		}

		Poco::File{file_path}.remove();

		LOG_TRACE(log, "Finished processing `" << file_path << '`');
	}

	std::string getLoggerName() const
	{
		return storage.name + '.' + storage.getName() + ".DirectoryMonitor";
	}

	StorageDistributed & storage;
	ConnectionPoolPtr pool;
	std::string path;
	std::size_t error_count{};
	std::chrono::milliseconds default_sleep_time;
	std::chrono::milliseconds sleep_time;
	std::chrono::time_point<std::chrono::system_clock> last_decrease_time{
		std::chrono::system_clock::now()
	};
	bool quit{false};
	std::mutex mutex;
	std::condition_variable cond;
	Logger * log;
	std::thread thread{&DirectoryMonitor::run, this};
};

}
