#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Client/ConnectionPool.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Common/isLocalAddress.h>
#include <ext/range.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include "writeParenthesisedString.h"


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
	ClickHouseDictionarySource(const DictionaryStructure & dict_struct,
		const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		const Block & sample_block, Context & context)
		: dict_struct{dict_struct},
		  host{config.getString(config_prefix + ".host")},
		  port(config.getInt(config_prefix + ".port")),
		  user{config.getString(config_prefix + ".user", "")},
		  password{config.getString(config_prefix + ".password", "")},
		  db{config.getString(config_prefix + ".db", "")},
		  table{config.getString(config_prefix + ".table")},
		  where{config.getString(config_prefix + ".where", "")},
		  sample_block{sample_block}, context(context),
		  is_local{isLocalAddress({ host, port })},
		  pool{is_local ? nullptr : std::make_unique<ConnectionPool>(
			  max_connections, host, port, db, user, password,
			  "ClickHouseDictionarySource")
		  },
		  load_all_query{composeLoadAllQuery()},
		  key_tuple_definition{dict_struct.key ? composeKeyTupleDefinition() : std::string{}}
	{}

	/// copy-constructor is provided in order to support cloneability
	ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
		: dict_struct{other.dict_struct},
		  host{other.host}, port{other.port}, user{other.user}, password{other.password},
		  db{other.db}, table{other.table},
		  where{other.where},
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
		return createStreamForSelectiveLoad(composeLoadIdsQuery(ids));
	}

	BlockInputStreamPtr loadKeys(
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows) override
	{
		return createStreamForSelectiveLoad(composeLoadKeysQuery(key_columns, requested_rows));
	}

	bool isModified() const override { return true; }
	bool supportsSelectiveLoad() const override { return true; }

	DictionarySourcePtr clone() const override { return std::make_unique<ClickHouseDictionarySource>(*this); }

	std::string toString() const override
	{
		return "ClickHouse: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
	}

private:
	std::string composeLoadAllQuery() const
	{
		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			if (dict_struct.id)
			{
				if (!dict_struct.id->expression.empty())
				{
					writeParenthesisedString(dict_struct.id->expression, out);
					writeString(" AS ", out);
				}

				writeProbablyBackQuotedString(dict_struct.id->name, out);

				if (dict_struct.range_min && dict_struct.range_max)
				{
					writeString(", ", out);

					if (!dict_struct.range_min->expression.empty())
					{
						writeParenthesisedString(dict_struct.range_min->expression, out);
						writeString(" AS ", out);
					}

					writeProbablyBackQuotedString(dict_struct.range_min->name, out);

					writeString(", ", out);

					if (!dict_struct.range_max->expression.empty())
					{
						writeParenthesisedString(dict_struct.range_max->expression, out);
						writeString(" AS ", out);
					}

					writeProbablyBackQuotedString(dict_struct.range_max->name, out);
				}
			}
			else if (dict_struct.key)
			{
				auto first = true;
				for (const auto & key : *dict_struct.key)
				{
					if (!first)
						writeString(", ", out);

					first = false;

					if (!key.expression.empty())
					{
						writeParenthesisedString(key.expression, out);
						writeString(" AS ", out);
					}

					writeProbablyBackQuotedString(key.name, out);
				}
			}

			for (const auto & attr : dict_struct.attributes)
			{
				writeString(", ", out);

				if (!attr.expression.empty())
				{
					writeParenthesisedString(attr.expression, out);
					writeString(" AS ", out);
				}

				writeProbablyBackQuotedString(attr.name, out);
			}

			writeString(" FROM ", out);
			if (!db.empty())
			{
				writeProbablyBackQuotedString(db, out);
				writeChar('.', out);
			}
			writeProbablyBackQuotedString(table, out);

			if (!where.empty())
			{
				writeString(" WHERE ", out);
				writeString(where, out);
			}

			writeChar(';', out);
		}

		return query;
	}

	std::string composeLoadIdsQuery(const std::vector<std::uint64_t> ids)
	{
		if (!dict_struct.id)
			throw Exception{"Simple key required for method", ErrorCodes::UNSUPPORTED_METHOD};

		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			if (!dict_struct.id->expression.empty())
			{
				writeParenthesisedString(dict_struct.id->expression, out);
				writeString(" AS ", out);
			}

			writeProbablyBackQuotedString(dict_struct.id->name, out);

			for (const auto & attr : dict_struct.attributes)
			{
				writeString(", ", out);

				if (!attr.expression.empty())
				{
					writeParenthesisedString(attr.expression, out);
					writeString(" AS ", out);
				}

				writeProbablyBackQuotedString(attr.name, out);
			}

			writeString(" FROM ", out);
			if (!db.empty())
			{
				writeProbablyBackQuotedString(db, out);
				writeChar('.', out);
			}
			writeProbablyBackQuotedString(table, out);

			writeString(" WHERE ", out);

			if (!where.empty())
			{
				writeString(where, out);
				writeString(" AND ", out);
			}

			writeProbablyBackQuotedString(dict_struct.id->name, out);
			writeString(" IN (", out);

			auto first = true;
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

	std::string composeLoadKeysQuery(
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
	{
		if (!dict_struct.key)
			throw Exception{"Composite key required for method", ErrorCodes::UNSUPPORTED_METHOD};

		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			auto first = true;
			for (const auto & key_or_attribute : boost::join(*dict_struct.key, dict_struct.attributes))
			{
				if (!first)
					writeString(", ", out);

				first = false;

				if (!key_or_attribute.expression.empty())
				{
					writeParenthesisedString(key_or_attribute.expression, out);
					writeString(" AS ", out);
				}

				writeProbablyBackQuotedString(key_or_attribute.name, out);
			}

			writeString(" FROM ", out);
			if (!db.empty())
			{
				writeProbablyBackQuotedString(db, out);
				writeChar('.', out);
			}
			writeProbablyBackQuotedString(table, out);

			writeString(" WHERE ", out);

			if (!where.empty())
			{
				writeString(where, out);
				writeString(" AND ", out);
			}

			writeString(key_tuple_definition, out);
			writeString(" IN (", out);

			first = true;
			for (const auto row : requested_rows)
			{
				if (!first)
					writeString(", ", out);

				first = false;
				composeKeyTuple(key_columns, row, out);
			}

			writeString(");", out);
		}

		return query;
	}

	std::string composeKeyTupleDefinition() const
	{
		if (!dict_struct.key)
			throw Exception{"Composite key required for method", ErrorCodes::UNSUPPORTED_METHOD};

		std::string result{"("};

		auto first = true;
		for (const auto & key : *dict_struct.key)
		{
			if (!first)
				result += ", ";

			first = false;
			result += key.name;
		}

		result += ")";

		return result;
	}

	void composeKeyTuple(const ConstColumnPlainPtrs & key_columns, const std::size_t row, WriteBuffer & out) const
	{
		writeString("(", out);

		const auto keys_size = key_columns.size();
		auto first = true;
		for (const auto i : ext::range(0, keys_size))
		{
			if (!first)
				writeString(", ", out);

			first = false;
			const auto & value = (*key_columns[i])[row];
			(*dict_struct.key)[i].type->serializeTextQuoted(value, out);
		}

		writeString(")", out);
	}

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
	Block sample_block;
	Context & context;
	const bool is_local;
	std::unique_ptr<ConnectionPool> pool;
	const std::string load_all_query;
	const std::string key_tuple_definition;
};

}
