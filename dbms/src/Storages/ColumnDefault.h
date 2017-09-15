#pragma once

#include <string>
#include <unordered_map>

#include <Parsers/IAST.h>


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
    template <> struct hash<DB::ColumnDefaultType>
    {
        size_t operator()(const DB::ColumnDefaultType type) const
        {
            return hash<int>{}(static_cast<int>(type));
        }
    };
}


namespace DB
{


ColumnDefaultType columnDefaultTypeFromString(const std::string & str);
std::string toString(const ColumnDefaultType type);


struct ColumnDefault
{
    ColumnDefaultType type;
    ASTPtr expression;
};


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);


using ColumnDefaults = std::unordered_map<std::string, ColumnDefault>;


}
