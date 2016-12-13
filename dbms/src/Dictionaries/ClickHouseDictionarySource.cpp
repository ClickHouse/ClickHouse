#include <DB/Dictionaries/ClickHouseDictionarySource.h>
#include <DB/Dictionaries/ExternalQueryBuilder.h>
#include <DB/Dictionaries/writeParenthesisedString.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Common/isLocalAddress.h>
#include <memory>
#include <ext/range.hpp>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNSUPPORTED_METHOD;
}


static const size_t max_connections = 16;


ClickHouseDictionarySource::ClickHouseDictionarySource(const DictionaryStructure & dict_struct_,
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
		pool{is_local ? nullptr : std::make_shared<ConnectionPool>(
			max_connections, host, port, db, user, password,
			"ClickHouseDictionarySource")
		},
		load_all_query{query_builder.composeLoadAllQuery()}
{}


ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
	: dict_struct{other.dict_struct},
		host{other.host}, port{other.port}, user{other.user}, password{other.password},
		db{other.db}, table{other.table},
		where{other.where},
		query_builder{dict_struct, db, table, where},
		sample_block{other.sample_block}, context(other.context),
		is_local{other.is_local},
		pool{is_local ? nullptr : std::make_shared<ConnectionPool>(
			max_connections, host, port, db, user, password,
			"ClickHouseDictionarySource")},
		load_all_query{other.load_all_query}
{}


BlockInputStreamPtr ClickHouseDictionarySource::loadAll()
{
	/** Query to local ClickHouse is marked internal in order to avoid
	  *	the necessity of holding process_list_element shared pointer.
	  */
	if (is_local)
		return executeQuery(load_all_query, context, true).in;
	return std::make_shared<RemoteBlockInputStream>(pool, load_all_query, nullptr);
}


BlockInputStreamPtr ClickHouseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	return createStreamForSelectiveLoad(
		query_builder.composeLoadIdsQuery(ids));
}


BlockInputStreamPtr ClickHouseDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	return createStreamForSelectiveLoad(
		query_builder.composeLoadKeysQuery(
			key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES));
}


std::string ClickHouseDictionarySource::toString() const
{
	return "ClickHouse: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}


BlockInputStreamPtr ClickHouseDictionarySource::createStreamForSelectiveLoad(const std::string query)
{
	if (is_local)
		return executeQuery(query, context, true).in;
	return std::make_shared<RemoteBlockInputStream>(pool, query, nullptr);
}

}
