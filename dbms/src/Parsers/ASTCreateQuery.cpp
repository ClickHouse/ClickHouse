#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST.h>

#include <Core/Types.h>

namespace DB
{

String ASTSource::getID(char) const
{
    return "Source definition";
}

ASTPtr ASTSource::clone() const
{
    auto res = std::make_shared<ASTSource>(*this);
    res->children.clear();

    if (source)
        res->set(res->source, source->clone());

    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    if (lifetime)
        res->set(res->lifetime, lifetime->clone());

    if (layout)
        res->set(res->layout, layout->clone());

    return res;
}

void ASTSource::formatImpl(const FormatSettings & settings,
                           FormatState & state,
                           FormatStateStacked frame) const
{
    if (source)
    {
        settings.ostr << (settings.hilite ? hilite_none : "");
        source->formatImpl(settings, state, frame);
    }

    if (primary_key)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws
                      << "PRIMARY KEY" << (settings.hilite ? hilite_none : "");
        primary_key->formatImpl(settings, state, frame);
    }

    lifetime->formatImpl(settings, state, frame);
    layout->formatImpl(settings, state, frame);
}

}