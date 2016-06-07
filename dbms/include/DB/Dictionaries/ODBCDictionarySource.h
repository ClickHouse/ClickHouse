#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/ODBCBlockInputStream.h>
#include <DB/Dictionaries/ExternalQueryBuilder.h>
#include <ext/range.hpp>
#include <mysqlxx/Pool.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Data/SessionPool.h>


namespace DB
{


/// Allows loading dictionaries from a ODBC source
class ODBCDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	ODBCDictionarySource(const DictionaryStructure & dict_struct_,
		const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		const Block & sample_block)
		: dict_struct{dict_struct_},
		  db{config.getString(config_prefix + ".db", "")},
		  table{config.getString(config_prefix + ".table")},
		  where{config.getString(config_prefix + ".where", "")},
		  sample_block{sample_block},
		  pool{std::make_shared<Poco::Data::SessionPool>(
			  config.getString(config_prefix + ".connector", "ODBC"),
			  config.getString(config_prefix + ".connection_string"))},
		  query_builder{dict_struct, db, table, where},
		  load_all_query{query_builder.composeLoadAllQuery()}
	{}

	/// copy-constructor is provided in order to support cloneability
	ODBCDictionarySource(const ODBCDictionarySource & other)
		: dict_struct{other.dict_struct},
		  db{other.db},
		  table{other.table},
		  where{other.where},
		  sample_block{other.sample_block},
		  pool{other.pool},
		  query_builder{dict_struct, db, table, where},
		  load_all_query{other.load_all_query}
	{}

	BlockInputStreamPtr loadAll() override
	{
		LOG_TRACE(log, load_all_query);
		return new ODBCBlockInputStream{pool->get(), load_all_query, sample_block, max_block_size};
	}

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> & ids) override
	{
		const auto query = query_builder.composeLoadIdsQuery(ids);
		return new ODBCBlockInputStream{pool->get(), query, sample_block, max_block_size};
	}

	BlockInputStreamPtr loadKeys(
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows) override
	{
		const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
		return new ODBCBlockInputStream{pool->get(), query, sample_block, max_block_size};
	}

	bool isModified() const override
	{
		return true;
	}

	bool supportsSelectiveLoad() const override { return true; }

	DictionarySourcePtr clone() const override { return std::make_unique<ODBCDictionarySource>(*this); }

	std::string toString() const override
	{
		return "ODBC: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
	}

private:
	Logger * log = &Logger::get("ODBCDictionarySource");

	const DictionaryStructure dict_struct;
	const std::string db;
	const std::string table;
	const std::string where;
	Block sample_block;
	std::shared_ptr<Poco::Data::SessionPool> pool;
	ExternalQueryBuilder query_builder;
	const std::string load_all_query;
};


}
