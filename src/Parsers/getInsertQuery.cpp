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
    query.table_id.database_name = db_name;
    query.table_id.table_name = table_name;
    query.columns = std::make_shared<ASTExpressionList>(',');
    query.children.push_back(query.columns);
    for (const auto & column : columns)
        query.columns->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

    WriteBufferFromOwnString buf;
    IAST::FormatSettings settings(buf, true);
    settings.always_quote_identifiers = true;
    settings.identifier_quoting_style = quoting;
    query.IAST::format(settings);
    return buf.str();
}
}
