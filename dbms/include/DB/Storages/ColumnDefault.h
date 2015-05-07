#pragma once

#include <DB/Parsers/queryToString.h>
#include <unordered_map>


namespace DB
{

enum class ColumnDefaultType
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


}
