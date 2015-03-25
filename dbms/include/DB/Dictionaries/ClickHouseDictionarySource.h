#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Common/isLocalAddress.h>
#include <statdaemons/ext/range.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

const auto max_connections = 16;

/** Allows loading dictionaries from local or remote ClickHouse instance
*	@todo use ConnectionPoolWithFailover
*	@todo invent a way to keep track of source modifications
*/
class ClickHouseDictionarySource final : public IDictionarySource
{
public:
	ClickHouseDictionarySource(const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		Block & sample_block, Context & context)
		: host{config.getString(config_prefix + ".host")},
		  port(config.getInt(config_prefix + ".port")),
		  user{config.getString(config_prefix + ".user", "")},
		  password{config.getString(config_prefix + ".password", "")},
		  db{config.getString(config_prefix + ".db", "")},
		  table{config.getString(config_prefix + ".table")},
		  sample_block{sample_block}, context(context),
		  is_local{isLocalAddress({ host, port })},
		  pool{is_local ? nullptr : std::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password, context.getDataTypeFactory(),
			  "ClickHouseDictionarySource")
		  },
		  load_all_query{composeLoadAllQuery(sample_block, db, table)}
	{}

	/// copy-constructor is provided in order to support cloneability
	ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
		: host{other.host}, port{other.port}, user{other.user}, password{other.password},
		  db{other.db}, table{other.table},
		  sample_block{other.sample_block}, context(other.context),
		  is_local{other.is_local},
		  pool{is_local ? nullptr : std::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password, context.getDataTypeFactory(),
			  "ClickHouseDictionarySource")},
		  load_all_query{other.load_all_query}
	{}

	BlockInputStreamPtr loadAll() override
	{
		/** Query to local ClickHouse is marked internal in order to avoid
		*	the necessity of holding process_list_element shared pointer.
		*/
		if (is_local)
			return executeQuery(load_all_query, context, true).in;
		return new RemoteBlockInputStream{pool.get(), load_all_query, nullptr};
	}

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> ids) override
	{
		const auto query = composeLoadIdsQuery(ids);

		if (is_local)
			return executeQuery(query, context, true).in;
		return new RemoteBlockInputStream{pool.get(), query, nullptr};
	}

	bool isModified() const override { return true; }
	bool supportsSelectiveLoad() const override { return true; }

	DictionarySourcePtr clone() const override { return std::make_unique<ClickHouseDictionarySource>(*this); }

	std::string toString() const override { return "ClickHouse: " + db + '.' + table; }

private:
	static std::string composeLoadAllQuery(const Block & block, const std::string & db, const std::string & table)
	{
		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			auto first = true;
			for (const auto idx : ext::range(0, block.columns()))
			{
				if (!first)
					writeString(", ", out);

				writeString(block.getByPosition(idx).name, out);
				first = false;
			}

			writeString(" FROM ", out);
			if (!db.empty())
			{
				writeProbablyBackQuotedString(db, out);
				writeChar('.', out);
			}
			writeProbablyBackQuotedString(table, out);
			writeChar(';', out);
		}

		return query;
	}

	std::string composeLoadIdsQuery(const std::vector<std::uint64_t> ids)
	{
		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			auto first = true;
			for (const auto idx : ext::range(0, sample_block.columns()))
			{
				if (!first)
					writeString(", ", out);

				writeString(sample_block.getByPosition(idx).name, out);
				first = false;
			}

			const auto & id_column_name = sample_block.getByPosition(0).name;
			writeString(" FROM ", out);
			if (!db.empty())
			{
				writeProbablyBackQuotedString(db, out);
				writeChar('.', out);
			}
			writeProbablyBackQuotedString(table, out);
			writeString(" WHERE ", out);
			writeProbablyBackQuotedString(id_column_name, out);
			writeString(" IN (", out);

			first = true;
			for (const auto id : ids)
			{
				if (!first)
					writeString(", ", out);

				first = false;
				writeString(DB::toString(id), out);
			}

			writeString(");", out);
		}

		return query;
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
