#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace
{
    using Kind = ASTDropAccessEntityQuery::Kind;

    const char * kindToKeyword(Kind kind)
    {
        switch (kind)
        {
            case Kind::QUOTA: return "QUOTA";
        }
        __builtin_unreachable();
    }
}


ASTDropAccessEntityQuery::ASTDropAccessEntityQuery(Kind kind_)
    : kind(kind_), keyword(kindToKeyword(kind_))
{
}


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + keyword + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    return std::make_shared<ASTDropAccessEntityQuery>(*this);
}


void ASTDropAccessEntityQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << keyword
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    bool need_comma = false;
    for (const auto & name : names)
    {
        if (need_comma)
            settings.ostr << ',';
        need_comma = true;
        settings.ostr << ' ' << backQuoteIfNeed(name);
    }
}
}
