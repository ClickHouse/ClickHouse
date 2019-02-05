#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

struct FindIdentifierBestTableData
{
    using TypeToVisit = ASTIdentifier;
    const std::vector<DatabaseAndTableWithAlias> & tables;
    std::vector<std::pair<ASTIdentifier *, const DatabaseAndTableWithAlias *>> identifier_table;

    FindIdentifierBestTableData(const std::vector<DatabaseAndTableWithAlias> & tables_);

    void visit(ASTIdentifier & identifier, ASTPtr &);
};

using FindIdentifierBestTableMatcher = OneTypeMatcher<FindIdentifierBestTableData>;
using FindIdentifierBestTableVisitor = InDepthNodeVisitor<FindIdentifierBestTableMatcher, true>;

}
