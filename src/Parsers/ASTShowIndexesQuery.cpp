#include <Parsers/ASTShowIndexesQuery.h>
#include <Common/SipHash.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTShowIndexesQuery::clone() const
{
    auto res = make_intrusive<ASTShowIndexesQuery>(*this);
    res->children.clear();
    /// `where_expression` is not a child: the parser puts it into the member only. Do not leave it
    /// shared with the source.
    if (where_expression)
        res->where_expression = where_expression->clone();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowIndexesQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `where_expression` is member-only in the parser and must be included explicitly.
    hash_state.update(extended);
    const auto update_string = [&hash_state](const String & value)
    {
        hash_state.update(value.size());
        hash_state.update(value);
    };

    update_string(database);
    update_string(table);
    hash_state.update(where_expression != nullptr);
    if (where_expression)
        where_expression->updateTreeHash(hash_state, ignore_aliases);
    ASTQueryWithOutput::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTShowIndexesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr
                  << "SHOW "
                  << (extended ? "EXTENDED " : "")
                  << "INDEXES"
                 ;

    ostr << " FROM " << backQuoteIfNeed(table);
    if (!database.empty())
        ostr << " FROM " << backQuoteIfNeed(database);

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }
}

}
