#pragma once

#include <ext/range.hpp>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Dictionaries/writeParenthesisedString.h>
#include <DB/Dictionaries/DictionaryStructure.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNSUPPORTED_METHOD;
}


/** Генерирует запрос для загрузки данных из внешней БД.
  */
struct ExternalQueryBuilder
{
	const DictionaryStructure & dict_struct;
	const std::string & db;
	const std::string & table;
	const std::string & where;


	ExternalQueryBuilder(
		const DictionaryStructure & dict_struct,
		const std::string & db,
		const std::string & table,
		const std::string & where)
		: dict_struct(dict_struct), db(db), table(table), where(where)
	{
	}

	/** Получить запрос на загрузку всех данных. */
	std::string composeLoadAllQuery() const
	{
		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			if (dict_struct.id)
			{
				if (!dict_struct.id.value().expression.empty())
				{
					writeParenthesisedString(dict_struct.id.value().expression, out);
					writeString(" AS ", out);
				}

				writeProbablyBackQuotedString(dict_struct.id.value().name, out);

				if (dict_struct.range_min && dict_struct.range_max)
				{
					writeString(", ", out);

					if (!dict_struct.range_min.value().expression.empty())
					{
						writeParenthesisedString(dict_struct.range_min.value().expression, out);
						writeString(" AS ", out);
					}

					writeProbablyBackQuotedString(dict_struct.range_min.value().name, out);

					writeString(", ", out);

					if (!dict_struct.range_max.value().expression.empty())
					{
						writeParenthesisedString(dict_struct.range_max.value().expression, out);
						writeString(" AS ", out);
					}

					writeProbablyBackQuotedString(dict_struct.range_max.value().name, out);
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

	/** Получить запрос на загрузку данных по множеству простых ключей. */
	std::string composeLoadIdsQuery(const std::vector<std::uint64_t> & ids)
	{
		if (!dict_struct.id)
			throw Exception{"Simple key required for method", ErrorCodes::UNSUPPORTED_METHOD};

		std::string query;

		{
			WriteBufferFromString out{query};
			writeString("SELECT ", out);

			if (!dict_struct.id.value().expression.empty())
			{
				writeParenthesisedString(dict_struct.id.value().expression, out);
				writeString(" AS ", out);
			}

			writeProbablyBackQuotedString(dict_struct.id.value().name, out);

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

			writeProbablyBackQuotedString(dict_struct.id.value().name, out);
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


	/** Получить запрос на загрузку данных по множеству сложных ключей.
	  * Есть два метода их указания в секции WHERE:
	  * 1. (x = c11 AND y = c12) OR (x = c21 AND y = c22) ...
	  * 2. (x, y) IN ((c11, c12), (c21, c22), ...)
	  */
	enum LoadKeysMethod
	{
		AND_OR_CHAIN,
		IN_WITH_TUPLES,
	};

	std::string composeLoadKeysQuery(
		const ConstColumnPlainPtrs & key_columns,
		const std::vector<std::size_t> & requested_rows,
		LoadKeysMethod method)
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
				writeString("(", out);
				writeString(where, out);
				writeString(") AND (", out);
			}

			if (method == AND_OR_CHAIN)
			{
				first = true;
				for (const auto row : requested_rows)
				{
					if (!first)
						writeString(" OR ", out);

					first = false;
					composeKeyCondition(key_columns, row, out);
				}
			}
			else if (method == IN_WITH_TUPLES)
			{
				writeString(composeKeyTupleDefinition(), out);
				writeString(" IN (", out);

				first = true;
				for (const auto row : requested_rows)
				{
					if (!first)
						writeString(", ", out);

					first = false;
					composeKeyTuple(key_columns, row, out);
				}

				writeString(")", out);
			}

			if (!where.empty())
			{
				writeString(")", out);
			}

			writeString(";", out);
		}

		return query;
	}


private:
	/// Выражение вида (x = c1 AND y = c2 ...)
	void composeKeyCondition(const ConstColumnPlainPtrs & key_columns, const std::size_t row, WriteBuffer & out) const
	{
		writeString("(", out);

		const auto keys_size = key_columns.size();
		auto first = true;
		for (const auto i : ext::range(0, keys_size))
		{
			if (!first)
				writeString(" AND ", out);

			first = false;

			const auto & key_description = (*dict_struct.key)[i];

			/// key_i=value_i
			writeString(key_description.name, out);
			writeString("=", out);
			key_description.type->serializeTextQuoted(*key_columns[i], row, out);
		}

		writeString(")", out);
	}

	/// Выражение вида (x, y, ...)
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

	/// Выражение вида (c1, c2, ...)
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
			(*dict_struct.key)[i].type->serializeTextQuoted(*key_columns[i], row, out);
		}

		writeString(")", out);
	}
};

}
