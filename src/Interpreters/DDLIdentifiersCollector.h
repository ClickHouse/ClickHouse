#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <set>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct DDLQueryIdentifiers
{
    std::set<std::string> used_identifiers;
    std::set<std::string> produced_identifiers;

    void addQueryIdentifiers(const DDLQueryIdentifiers & other);
    void removeQueryIdentifiers(const DDLQueryIdentifiers & other);
};

bool hasIntersection(const DDLQueryIdentifiers & left, const DDLQueryIdentifiers & right);

DDLQueryIdentifiers collectIdentifiersForDDLQuery(const ASTPtr & ddl_query, ContextPtr context);

}
