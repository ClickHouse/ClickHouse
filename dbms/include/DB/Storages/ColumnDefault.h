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

	using ColumnDefaults = std::unordered_map<String, ColumnDefault>;

	template <bool store>
	struct ColumnsDescription
	{
		template <typename T> using by_value_or_cref = typename std::conditional<store, T, const T &>::type;
		by_value_or_cref<NamesAndTypesList> columns;
		by_value_or_cref<NamesAndTypesList> materialized;
		by_value_or_cref<NamesAndTypesList> alias;
		by_value_or_cref<ColumnDefaults> defaults;

		String toString() const
		{
			String s;
			WriteBufferFromString buf{s};

			writeString("columns format version: 1\n", buf);
			writeText(columns.size() + materialized.size() + alias.size(), buf);
			writeString(" columns:\n", buf);

			const auto write_columns = [this, &buf] (const NamesAndTypesList & columns) {
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

		static ColumnsDescription parse(const String & str, const DataTypeFactory & data_type_factory)
		{
			ReadBufferFromString buf{str};

			assertString("columns format version: 1\n", buf);
			size_t count{};
			readText(count, buf);
			assertString(" columns:\n", buf);

			ParserTernaryOperatorExpression expr_parser;

			ColumnsDescription result{};
			for (size_t i = 0; i < count; ++i)
			{
				String column_name;
				readBackQuotedString(column_name, buf);
				assertString(" ", buf);

				String type_name;
				readString(type_name, buf);
				auto type = data_type_factory.get(type_name);
				if (*buf.position() == '\n')
				{
					assertString("\n", buf);

					result.columns.emplace_back(column_name, std::move(type));
					continue;
				}
				assertString("\t", buf);

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
	};
}
