#include "TableNameOrQuery.h"

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

const String & TableNameOrQuery::getTableName() const
{
    if (type != Type::TABLE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get table name from DataQueryTarget: {}", format());
    return target;
}

const String & TableNameOrQuery::getQuery() const
{
    if (type != Type::QUERY)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get query string from DataQueryTarget: {}", format());
    return target;
}

String TableNameOrQuery::format() const
{
    String reflection;
    switch (type)
    {
        case Type::TABLE:
            reflection += "Table: ";
            break;
        case Type::QUERY:
            reflection += "Query: ";
            break;
    }
    reflection += target;
    return reflection;
}

}
