#include <Parsers/ASTAnalyzeQuery.h>

namespace DB
{

ASTPtr ASTAnalyzeQuery::clone() const
{
    auto res = std::make_shared<ASTAnalyzeQuery>();

    res->children.clear();
    res->is_full = is_full;
    res->is_async = is_async;
    res->cluster = cluster;

    if (database)
    {
        res->database = database->clone();
        res->children.push_back(res->database);
    }
    if (table)
    {
        res->table = table->clone();
        res->children.push_back(res->table);
    }
    if (column_list)
    {
        res->column_list = column_list->clone();
        res->children.push_back(res->column_list);
    }
    if (settings)
    {
        res->settings = settings->clone();
        res->children.push_back(res->settings);
    }

    return res;
}

void ASTAnalyzeQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    char ws = ' ';
    s.ostr << (s.hilite ? hilite_keyword : "") << "ANALYZE" << (s.hilite ? hilite_keyword : "") << ws;
    s.ostr << (is_full ? "FULL" : "SAMPLE") << s.nl_or_ws;
    s.ostr << (s.hilite ? hilite_keyword : "") << "TABLE" << (s.hilite ? hilite_keyword : "") << ws;

    if (database)
    {
        database->formatImpl(s, state, frame);
        s.ostr << ".";
    }
    if (table)
    {
        table->formatImpl(s, state, frame);
    }
    if (column_list)
    {
        s.ostr << "(";
        column_list->formatImpl(s, state, frame);
        s.ostr << ")";
    }
    if (!cluster.empty())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << ws << "ON CLUSTER " << (s.hilite ? hilite_none : "");
        s.ostr << cluster;
    }
    if (is_async)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << ws << "ASYNC" << (s.hilite ? hilite_none : "");
    }
    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }

}

}
