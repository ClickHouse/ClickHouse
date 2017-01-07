#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/queryToString.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/Storages/ColumnsDescription.h>
#include <DB/DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_PARSE_TEXT;
}


template <bool store>
String ColumnsDescription<store>::toString() const
{
	String s;
	WriteBufferFromString buf{s};

	writeString("columns format version: 1\n", buf);
	writeText(columns.size() + materialized.size() + alias.size(), buf);
	writeString(" columns:\n", buf);

	const auto write_columns = [this, &buf] (const NamesAndTypesList & columns)
	{
		for (const auto & column : columns)
		{
			const auto it = defaults.find(column.name);

			writeBackQuotedString(column.name, buf);
			writeChar(' ', buf);
			writeString(column.type->getName(), buf);
			if (it == std::end(defaults))
			{
				writeChar('\n', buf);
				continue;
			}
			else
				writeChar('\t', buf);

			writeString(DB::toString(it->second.type), buf);
			writeChar('\t', buf);
			writeString(queryToString(it->second.expression), buf);
			writeChar('\n', buf);
		}
	};

	write_columns(columns);
	write_columns(materialized);
	write_columns(alias);

	return s;
}


template <>
ColumnsDescription<true> ColumnsDescription<true>::parse(const String & str)
{
	ReadBufferFromString buf{str};

	assertString("columns format version: 1\n", buf);
	size_t count{};
	readText(count, buf);
	assertString(" columns:\n", buf);

	ParserTernaryOperatorExpression expr_parser;
	const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

	ColumnsDescription<true> result{};
	for (size_t i = 0; i < count; ++i)
	{
		String column_name;
		readBackQuotedString(column_name, buf);
		assertChar(' ', buf);

		String type_name;
		readString(type_name, buf);
		auto type = data_type_factory.get(type_name);
		if (*buf.position() == '\n')
		{
			assertChar('\n', buf);

			result.columns.emplace_back(column_name, std::move(type));
			continue;
		}
		assertChar('\t', buf);

		String default_type_str;
		readString(default_type_str, buf);
		const auto default_type = columnDefaultTypeFromString(default_type_str);
		assertChar('\t', buf);

		String default_expr_str;
		readText(default_expr_str, buf);
		assertChar('\n', buf);

		ASTPtr default_expr;
		Expected expected{};
		auto begin = default_expr_str.data();
		const auto end = begin + default_expr_str.size();
		const char * max_parsed_pos = begin;
		if (!expr_parser.parse(begin, end, default_expr, max_parsed_pos, expected))
			throw Exception{"Could not parse default expression", DB::ErrorCodes::CANNOT_PARSE_TEXT};

		if (ColumnDefaultType::Default == default_type)
			result.columns.emplace_back(column_name, std::move(type));
		else if (ColumnDefaultType::Materialized == default_type)
			result.materialized.emplace_back(column_name, std::move(type));
		else if (ColumnDefaultType::Alias == default_type)
			result.alias.emplace_back(column_name, std::move(type));

		result.defaults.emplace(column_name, ColumnDefault{default_type, default_expr});
	}

	assertEOF(buf);

	return result;
}


template struct ColumnsDescription<false>;
template struct ColumnsDescription<true>;

}
