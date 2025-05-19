#pragma once

#include <Core/Types.h>

namespace DB
{

class TableNameOrQuery
{
public:
    enum class Type
    {
        TABLE,
        QUERY,
    };
    TableNameOrQuery() : type(Type::TABLE) {}
    TableNameOrQuery(Type type_, const String & target_) : type(type_), target(target_) {}

    Type getType() const { return type; }

    const String & getTableName() const;
    const String & getQuery() const;

    bool isTable() const { return type == Type::TABLE; }
    bool isQuery() const { return type == Type::QUERY; }

    String format() const;

private:
    Type type;
    String target;
};

}
