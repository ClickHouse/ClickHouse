#include <Parsers/getInsertQuery.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{
std::string getInsertQuery(const std::string & db_name, const std::string & table_name, const ColumnsWithTypeAndName & columns, IdentifierQuotingStyle quoting)
{
    ASTInsertQuery query;
    /// The destination goes through identifier nodes, not `table_id`, so that it too is formatted in `quoting`.
    query.setDatabase(db_name);
    query.setTable(table_name);
    query.columns = make_intrusive<ASTExpressionList>(',');
    query.children.push_back(query.columns);
    for (const auto & column : columns)
        query.columns->children.emplace_back(make_intrusive<ASTIdentifier>(column.name));

    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(
        /*one_line=*/true,
        /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
        /*identifier_quoting_style=*/quoting);
    query.IAST::format(buf, settings);
    return buf.str();
}
}
