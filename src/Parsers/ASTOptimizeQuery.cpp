#include <Parsers/ASTOptimizeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

String ASTOptimizeQuery::getID(char delim) const
{
    return "OptimizeQuery" + (delim + getDatabase()) + delim + getTable() + (final ? "_final" : "") + (deduplicate ? "_deduplicate" : "")+ (cleanup ? "_cleanup" : "");
}

ASTPtr ASTOptimizeQuery::clone() const
{
    auto res = std::make_shared<ASTOptimizeQuery>(*this);
    res->children.clear();

    if (partition)
        res->set(res->partition, partition->clone());
    if (deduplicate_by_columns)
        res->set(res->deduplicate_by_columns, deduplicate_by_columns->clone());

    return res;
}

void ASTOptimizeQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (settings.hilite ? hilite_none : "");

    if (database)
    {
        database->formatImpl(settings, state, frame);
        settings.ostr << '.';
    }

    chassert(table);
    table->formatImpl(settings, state, frame);

    formatOnCluster(settings);

    if (partition)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }

    if (final)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FINAL" << (settings.hilite ? hilite_none : "");

    if (deduplicate)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " DEDUPLICATE" << (settings.hilite ? hilite_none : "");

    if (cleanup)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CLEANUP" << (settings.hilite ? hilite_none : "");

    if (deduplicate_by_columns)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " BY " << (settings.hilite ? hilite_none : "");
        deduplicate_by_columns->formatImpl(settings, state, frame);
    }
}

ASTPtr ASTOptimizeQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    return removeOnCluster<ASTOptimizeQuery>(clone(), params.default_database);
}

}
