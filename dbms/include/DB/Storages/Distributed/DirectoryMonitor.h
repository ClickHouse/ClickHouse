#pragma once

#include <DB/DataStreams/RemoteBlockOutputStream.h>
#include <DB/Common/escapeForFileName.h>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/finder.hpp>

namespace DB
{

namespace
{
	template <typename F> ConnectionPools createPoolsForAddresses(const std::string & name, F && f)
	{
		ConnectionPools pools;

		for (auto it = boost::make_split_iterator(name, boost::first_finder(",")); it != decltype(it){}; ++it)
		{
			const auto & address = boost::copy_range<std::string>(*it);

			const auto user_pw_end = strchr(address.data(), '@');
			const auto colon = strchr(address.data(), ':');
			if (!user_pw_end || !colon)
				throw Exception{"Shard address '" + address + "' does not match to 'user[:password]@host:port' pattern"};

			const auto has_pw = colon < user_pw_end;
			const auto host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
			if (!host_end)
				throw Exception{"Shard address '" + address + "' does not contain port"};

			const auto user = unescapeForFileName({address.data(), has_pw ? colon : user_pw_end});
			const auto password = has_pw ? unescapeForFileName({colon + 1, user_pw_end}) : std::string{};
			const auto host = unescapeForFileName({user_pw_end + 1, host_end});
			const auto port = DB::parse<UInt16>(host_end + 1);

			pools.emplace_back(f(host, port, user, password));
		}

		/// just to be explicit
		return std::move(pools);
	}
}

class DirectoryMonitor
{
public:
	DirectoryMonitor(StorageDistributed & storage, const std::string & name)
	: storage(storage), pool{createPool(name)}, path{storage.path + name + '/'}
	, sleep_time{storage.context.getSettingsRef().distributed_directory_monitor_sleep_time_ms.totalMilliseconds()}
	, log{&Logger::get(getLoggerName())}
	{
	}

	void run()
	{
		while (!storage.quit.load(std::memory_order_relaxed))
		{
			auto no_work = true;
			auto exception = false;

			try
			{
				no_work = !findFiles();
			}
			catch (...)
			{
				exception = true;
				tryLogCurrentException(getLoggerName().data());
			}

			if (no_work || exception)
				std::this_thread::sleep_for(sleep_time);
		}
	}

private:
	ConnectionPoolPtr createPool(const std::string & name)
	{
		const auto pool_factory = [this, &name] (const std::string & host, const UInt16 port, const std::string & user, const std::string & password) {
			return new ConnectionPool{
				1, host, port, "",
				user, password, storage.context.getDataTypeFactory(),
				storage.getName() + '_' + name
			};
		};

		auto pools = createPoolsForAddresses(name, pool_factory);

		return pools.size() == 1 ? pools.front() : new ConnectionPoolWithFailover(pools, DB::LoadBalancing::RANDOM);
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
				files[DB::parse<UInt64>(file_path.getBaseName())] = file_path_str;
		}

		if (files.empty())
			return false;

		for (const auto & file : files)
		{
			if (storage.quit.load(std::memory_order_relaxed))
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
			DB::ReadBufferFromFile in{file_path};

			std::string insert_query;
			DB::readStringBinary(insert_query, in);

			DB::RemoteBlockOutputStream remote{*connection, insert_query};

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

				Poco::File{broken_path}.createDirectory();
				Poco::File{file_path}.moveTo(broken_path + file_name);

				LOG_ERROR(log, "Moved `" << file_path << "` to broken/ directory");
			}

			throw;
		}

		Poco::File{file_path}.remove();

		LOG_TRACE(log, "Finished processing `" << file_path << '`');
	}

	std::string getLoggerName() const {
		return storage.name + '.' + storage.getName() + ".DirectoryMonitor";
	}

	StorageDistributed & storage;
	ConnectionPoolPtr pool;
	std::string path;
	std::chrono::milliseconds sleep_time;

	Logger * log;
};

}
