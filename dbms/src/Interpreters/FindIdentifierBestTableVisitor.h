#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

struct FindIdentifierBestTableData
{
    using TypeToVisit = ASTIdentifier;
    using IdentifierWithTable = std::pair<ASTIdentifier *, const DatabaseAndTableWithAlias *>;

    const std::vector<TableWithColumnNames> & tables;
    std::vector<IdentifierWithTable> identifier_table;

    FindIdentifierBestTableData(const std::vector<TableWithColumnNames> & tables_);

    void visit(ASTIdentifier & identifier, ASTPtr &);
};

using FindIdentifierBestTableMatcher = OneTypeMatcher<FindIdentifierBestTableData>;
using FindIdentifierBestTableVisitor = InDepthNodeVisitor<FindIdentifierBestTableMatcher, true>;

}
