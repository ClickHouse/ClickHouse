#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <unordered_map>

namespace DB
{
	enum struct ColumnDefaultType
	{
		Default,
		Materialized,
		Alias
	};
}

namespace std
{
	template<> struct hash<DB::ColumnDefaultType>
	{
		size_t operator()(const DB::ColumnDefaultType type) const
		{
			return hash<int>{}(static_cast<int>(type));
		}
	};
}

namespace DB
{
	inline ColumnDefaultType columnDefaultTypeFromString(const String & str)
	{
		static const std::unordered_map<String, ColumnDefaultType> map{
			{ "DEFAULT", ColumnDefaultType::Default },
			{ "MATERIALIZED", ColumnDefaultType::Materialized },
			{ "ALIAS", ColumnDefaultType::Alias }
		};

		const auto it = map.find(str);
		return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str};
	}

	inline String toString(const ColumnDefaultType type)
	{
		static const std::unordered_map<ColumnDefaultType, String> map{
			{ ColumnDefaultType::Default, "DEFAULT" },
			{ ColumnDefaultType::Materialized, "MATERIALIZED" },
			{ ColumnDefaultType::Alias, "ALIAS" }
		};

		const auto it = map.find(type);
		return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultType"};
	}

	struct ColumnDefault
	{
		ColumnDefaultType type;
		ASTPtr expression;
	};

	inline bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
	{
		return lhs.type == rhs.type && queryToString(lhs.expression) == queryToString(rhs.expression);
	}

	struct ColumnDefaults : public std::unordered_map<String, ColumnDefault>
	{
		using std::unordered_map<String, ColumnDefault>::unordered_map;

		/// @todo implement (de)serialization
		String toString() const
		{
			String s;
			WriteBufferFromString buf{s};

			writeString("column defaults format version: 1\n", buf);
			writeText(size(), buf);
			writeString(" columns:\n", buf);

			for (const auto & column_default : *this)
			{
				writeBackQuotedString(column_default.first, buf);
				writeChar(' ', buf);
				writeString(DB::toString(column_default.second.type), buf);
				writeChar('\t', buf);
				writeString(queryToString(column_default.second.expression), buf);
				writeChar('\n', buf);
			}

			return s;
		}

		static ColumnDefaults parse(const String & str) {
			ReadBufferFromString buf{str};
			ColumnDefaults defaults{};

			assertString("column defaults format version: 1\n", buf);
			size_t count{};
			readText(count, buf);
			assertString(" columns:\n", buf);

			ParserTernaryOperatorExpression expr_parser;

			for (size_t i = 0; i < count; ++i)
			{
				String column_name;
				readBackQuotedString(column_name, buf);
				assertString(" ", buf);

				String default_type_str;
				readString(default_type_str, buf);
				const auto default_type = columnDefaultTypeFromString(default_type_str);
				assertString("\t", buf);

				String default_expr_str;
				readText(default_expr_str, buf);
				assertString("\n", buf);

				ASTPtr default_expr;
				Expected expected{};
				auto begin = default_expr_str.data();
				const auto end = begin + default_expr_str.size();
				if (!expr_parser.parse(begin, end, default_expr, expected))
					throw Exception{"Could not parse default expression", DB::ErrorCodes::CANNOT_PARSE_TEXT};

				defaults.emplace(column_name, ColumnDefault{default_type, default_expr});
			}

			assertEOF(buf);

			return defaults;
		}
	};
}
