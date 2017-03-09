#include <DB/Parsers/queryToString.h>
#include <DB/Storages/ColumnDefault.h>


namespace DB
{


ColumnDefaultType columnDefaultTypeFromString(const std::string & str)
{
	static const std::unordered_map<std::string, ColumnDefaultType> map{
		{ "DEFAULT", ColumnDefaultType::Default },
		{ "MATERIALIZED", ColumnDefaultType::Materialized },
		{ "ALIAS", ColumnDefaultType::Alias }
	};

	const auto it = map.find(str);
	return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str};
}


std::string toString(const ColumnDefaultType type)
{
	static const std::unordered_map<ColumnDefaultType, std::string> map{
		{ ColumnDefaultType::Default, "DEFAULT" },
		{ ColumnDefaultType::Materialized, "MATERIALIZED" },
		{ ColumnDefaultType::Alias, "ALIAS" }
	};

	const auto it = map.find(type);
	return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultType"};
}


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
	return lhs.type == rhs.type && queryToString(lhs.expression) == queryToString(rhs.expression);
}

}
