#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Client/ConnectionPool.h>
#include <statdaemons/ext/range.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/executeQuery.h>
#include <Poco/Net/NetworkInterface.h>

namespace DB
{

class ClickhouseDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;
	static const auto max_connections = 1;

public:
	ClickhouseDictionarySource(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block, const Context & context)
		: host{config.getString(config_prefix + "host")},
		  port(config.getInt(config_prefix + "port")),
		  is_local{isLocal(host, port)},
		  pool{is_local ? nullptr : ext::make_unique<ConnectionPool>(
			max_connections, host, port,
			config.getString(config_prefix + "db", ""),
			config.getString(config_prefix + "user", ""),
			config.getString(config_prefix + "password", ""),
			context.getDataTypeFactory(),
			"ClickhouseDictionarySource")
		  },
		  sample_block{sample_block}, context(context),
		  table{config.getString(config_prefix + "table")},
		  load_all_query{composeLoadAllQuery(sample_block, table)}
	{}

private:
	BlockInputStreamPtr loadAll() override
	{
		if (is_local)
			return executeQuery(load_all_query, context).in;
		return new RemoteBlockInputStream{pool.get(), load_all_query, nullptr};
	}

	BlockInputStreamPtr loadId(const std::uint64_t id) override
	{
		throw Exception{
			"Method unsupported",
			ErrorCodes::NOT_IMPLEMENTED
		};
	}

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> ids) override
	{
		throw Exception{
			"Method unsupported",
			ErrorCodes::NOT_IMPLEMENTED
		};
	}

	/// @todo check update_time with SHOW TABLE STATUS LIKE '%table%'
	bool isModified() const override { return true; }


	static std::string composeLoadAllQuery(const Block & block, const std::string & table)
	{
		std::string query{"SELECT "};

		auto first = true;
		for (const auto idx : ext::range(0, block.columns()))
		{
			if (!first)
				query += ", ";

			query += block.getByPosition(idx).name;
			first = false;
		}

		query += " FROM " + table + ';';

		return query;
	}

	static bool isLocal(const std::string & host, const UInt16 port)
	{
		const UInt16 clickhouse_port = Poco::Util::Application::instance().config().getInt("tcp_port", 0);
		static auto interfaces = Poco::Net::NetworkInterface::list();

		if (clickhouse_port == port)
		{
			return interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
				[&] (const Poco::Net::NetworkInterface & interface) {
					return interface.address() == Poco::Net::IPAddress(host);
				});
		}

		return false;
	}

	const std::string host;
	const UInt16 port;
	const bool is_local;
	std::unique_ptr<ConnectionPool> pool;
	Block sample_block;
	Context context;
	const std::string table;
	const std::string load_all_query;
};

}
