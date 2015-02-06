#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>
#include <statdaemons/ext/range.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/NetworkInterface.h>

namespace DB
{

const auto max_connections = 1;

class ClickhouseDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	ClickhouseDictionarySource(const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		Block & sample_block, Context & context)
		: host{config.getString(config_prefix + "host")},
		  port(config.getInt(config_prefix + "port")),
		  user{config.getString(config_prefix + "user", "")},
		  password{config.getString(config_prefix + "password", "")},
		  db{config.getString(config_prefix + "db", "")},
		  table{config.getString(config_prefix + "table")},
		  sample_block{sample_block}, context(context),
		  is_local{isLocal(host, port)},
		  pool{is_local ? nullptr : ext::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password, context.getDataTypeFactory(),
			  "ClickhouseDictionarySource")
		  },
		  load_all_query{composeLoadAllQuery(sample_block, table)}
	{}

	ClickhouseDictionarySource(const ClickhouseDictionarySource & other)
		: host{other.host}, port{other.port}, user{other.user}, password{other.password},
		  db{other.db}, table{other.db},
		  sample_block{other.sample_block}, context(other.context),
		  is_local{other.is_local},
		  pool{is_local ? nullptr : ext::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password, context.getDataTypeFactory(),
			  "ClickhouseDictionarySource")},
		  load_all_query{other.load_all_query}
	{}

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

	/// @todo check update time somehow
	bool isModified() const override { return true; }

	DictionarySourcePtr clone() const override { return ext::make_unique<ClickhouseDictionarySource>(*this); }

private:
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
	const std::string user;
	const std::string password;
	const std::string db;
	const std::string table;
	Block sample_block;
	Context & context;
	const bool is_local;
	std::unique_ptr<ConnectionPool> pool;
	const std::string load_all_query;
};

}
