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
	MysqlDictionarySource(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block, const Context & context)
		: table{config.getString(config_prefix + ".table")},
		  sample_block{sample_block}, context(context),
		  pool{config, config_prefix},
		  load_all_query{composeLoadAllQuery(sample_block, table)},
		  last_modification{getLastModification()}
	{}

	MysqlDictionarySource(const MysqlDictionarySource & other)
		: table{other.table},
		  sample_block{other.sample_block}, context(other.context),
		  pool{other.pool},
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
		const auto Update_time_idx = 12;

		try
		{
			auto connection = pool.Get();
			auto query = connection->query("SHOW TABLE STATUS LIKE '%" + table + "%';");
			auto result = query.use();
			auto row = result.fetch();
			const auto & update_time = row[Update_time_idx];
			return !update_time.isNull() ? update_time.getDateTime() : mysqlxx::DateTime{std::time(nullptr)};
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

	const std::string table;
	Block sample_block;
	const Context & context;
	mutable mysqlxx::PoolWithFailover pool;
	const std::string load_all_query;
	mysqlxx::DateTime last_modification;
};

}
