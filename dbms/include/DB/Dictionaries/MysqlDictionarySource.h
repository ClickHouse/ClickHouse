#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/MysqlBlockInputStream.h>
#include <DB/Interpreters/Context.h>
#include <statdaemons/ext/range.hpp>
#include <mysqlxx/Pool.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class MysqlDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	MysqlDictionarySource(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block, const Context & context)
		: host{config.getString(config_prefix + "host")},
		  port(config.getInt(config_prefix + "port")),
		  user{config.getString(config_prefix + "user", "")},
		  password{config.getString(config_prefix + "password", "")},
		  db{config.getString(config_prefix + "db", "")},
		  table{config.getString(config_prefix + "table")},
		  sample_block{sample_block}, context(context),
		  pool{db, host, user, password, port},
		  load_all_query{composeLoadAllQuery(sample_block, table)},
		  last_modification{getLastModification()}
	{}

	MysqlDictionarySource(const MysqlDictionarySource & other)
		: host{other.host}, port{other.port}, user{other.user}, password{other.password},
		  db{other.db}, table{other.db},
		  sample_block{other.sample_block}, context(other.context),
		  pool{db, host, user, password, port},
		  load_all_query{other.load_all_query}, last_modification{other.last_modification}
	{}

	BlockInputStreamPtr loadAll() override
	{
		return new MysqlBlockInputStream{pool.Get()->query(load_all_query), sample_block, max_block_size};
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

	bool isModified() const override { return getLastModification() > last_modification; }

	DictionarySourcePtr clone() const override { return ext::make_unique<MysqlDictionarySource>(*this); }

private:
	mysqlxx::DateTime getLastModification() const
	{
		const auto Create_time_idx = 11;
		const auto Update_time_idx = 12;

		try
		{
			auto connection = pool.Get();
			auto query = connection->query("SHOW TABLE STATUS LIKE '%" + table + "%';");
			auto result = query.use();
			auto row = result.fetch();
			const auto & update_time = row[Update_time_idx];
			return !update_time.isNull() ? update_time.getDateTime() : row[Create_time_idx].getDateTime();
		}
		catch (...)
		{
			tryLogCurrentException("MysqlDictionarySource");
		}

		return {};
	}

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

	const std::string host;
	const UInt16 port;
	const std::string user;
	const std::string password;
	const std::string db;
	const std::string table;
	Block sample_block;
	const Context & context;
	mutable mysqlxx::Pool pool;
	const std::string load_all_query;
	mysqlxx::DateTime last_modification;
};

}
