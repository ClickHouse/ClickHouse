#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/ExternalQueryBuilder.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Common/isLocalAddress.h>
#include <ext/range.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include "writeParenthesisedString.h"


namespace DB
{

namespace ErrorCodes
{
	extern const int UNSUPPORTED_METHOD;
}


const auto max_connections = 16;

/** Allows loading dictionaries from local or remote ClickHouse instance
*	@todo use ConnectionPoolWithFailover
*	@todo invent a way to keep track of source modifications
*/
class ClickHouseDictionarySource final : public IDictionarySource
{
public:
	ClickHouseDictionarySource(const DictionaryStructure & dict_struct_,
		const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		const Block & sample_block, Context & context)
		: dict_struct{dict_struct_},
		  host{config.getString(config_prefix + ".host")},
		  port(config.getInt(config_prefix + ".port")),
		  user{config.getString(config_prefix + ".user", "")},
		  password{config.getString(config_prefix + ".password", "")},
		  db{config.getString(config_prefix + ".db", "")},
		  table{config.getString(config_prefix + ".table")},
		  where{config.getString(config_prefix + ".where", "")},
		  query_builder{dict_struct, db, table, where},
		  sample_block{sample_block}, context(context),
		  is_local{isLocalAddress({ host, port })},
		  pool{is_local ? nullptr : std::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password,
			  "ClickHouseDictionarySource")
		  },
		  load_all_query{query_builder.composeLoadAllQuery()}
	{}

	/// copy-constructor is provided in order to support cloneability
	ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
		: dict_struct{other.dict_struct},
		  host{other.host}, port{other.port}, user{other.user}, password{other.password},
		  db{other.db}, table{other.table},
		  where{other.where},
		  query_builder{dict_struct, db, table, where},
		  sample_block{other.sample_block}, context(other.context),
		  is_local{other.is_local},
		  pool{is_local ? nullptr : std::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password,
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

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> & ids) override
	{
		return createStreamForSelectiveLoad(
			query_builder.composeLoadIdsQuery(ids));
	}

	BlockInputStreamPtr loadKeys(
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows) override
	{
		return createStreamForSelectiveLoad(
			query_builder.composeLoadKeysQuery(
				key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES));
	}

	bool isModified() const override { return true; }
	bool supportsSelectiveLoad() const override { return true; }

	DictionarySourcePtr clone() const override { return std::make_unique<ClickHouseDictionarySource>(*this); }

	std::string toString() const override
	{
		return "ClickHouse: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
	}

private:

	BlockInputStreamPtr createStreamForSelectiveLoad(const std::string query)
	{
		if (is_local)
			return executeQuery(query, context, true).in;
		return new RemoteBlockInputStream{pool.get(), query, nullptr};
	}


	const DictionaryStructure dict_struct;
	const std::string host;
	const UInt16 port;
	const std::string user;
	const std::string password;
	const std::string db;
	const std::string table;
	const std::string where;
	ExternalQueryBuilder query_builder;
	Block sample_block;
	Context & context;
	const bool is_local;
	std::unique_ptr<ConnectionPool> pool;
	const std::string load_all_query;
};

}
