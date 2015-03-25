#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/MySQLBlockInputStream.h>
#include <statdaemons/ext/range.hpp>
#include <mysqlxx/Pool.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <strconvert/escape.h>

namespace DB
{

/// Allows loading dictionaries from a MySQL database
class MySQLDictionarySource final : public IDictionarySource
{
	static const auto max_block_size = 8192;

public:
	MySQLDictionarySource(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		Block & sample_block)
		: db{config.getString(config_prefix + ".db", "")},
		  table{config.getString(config_prefix + ".table")},
		  sample_block{sample_block},
		  pool{config, config_prefix},
		  load_all_query{composeLoadAllQuery(sample_block, db, table)},
		  last_modification{getLastModification()}
	{}

	/// copy-constructor is provided in order to support cloneability
	MySQLDictionarySource(const MySQLDictionarySource & other)
		: db{other.db},
		  table{other.table},
		  sample_block{other.sample_block},
		  pool{other.pool},
		  load_all_query{other.load_all_query}, last_modification{other.last_modification}
	{}

	BlockInputStreamPtr loadAll() override
	{
		last_modification = getLastModification();
		return new MySQLBlockInputStream{pool.Get(), load_all_query, sample_block, max_block_size};
	}

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> ids) override
	{
		last_modification = getLastModification();
		const auto query = composeLoadIdsQuery(ids);

		return new MySQLBlockInputStream{pool.Get(), query, sample_block, max_block_size};
	}

	bool isModified() const override { return getLastModification() > last_modification; }
	bool supportsSelectiveLoad() const override { return true; }

	DictionarySourcePtr clone() const override { return std::make_unique<MySQLDictionarySource>(*this); }

	std::string toString() const override { return "MySQL: " + db + '.' + table; }

private:
	mysqlxx::DateTime getLastModification() const
	{
		const auto Update_time_idx = 12;
		mysqlxx::DateTime update_time{std::time(nullptr)};

		try
		{
			auto connection = pool.Get();
			auto query = connection->query("SHOW TABLE STATUS LIKE '%" + strconvert::escaped_for_like(table) + "%';");
			auto result = query.use();

			if (auto row = result.fetch())
			{
				const auto & update_time_value = row[Update_time_idx];

				if (!update_time_value.isNull())
					update_time = update_time_value.getDateTime();

				/// fetch remaining rows to avoid "commands out of sync" error
				while (auto row = result.fetch());
			}
		}
		catch (...)
		{
			tryLogCurrentException("MySQLDictionarySource");
		}

		/// we suppose failure to get modification time is not an error, therefore return current time
		return update_time;
	}

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

	const std::string db;
	const std::string table;
	Block sample_block;
	mutable mysqlxx::PoolWithFailover pool;
	const std::string load_all_query;
	mysqlxx::DateTime last_modification;
};

}
