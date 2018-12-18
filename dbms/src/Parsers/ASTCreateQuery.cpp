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

    res->min_lifetime = min_lifetime;
    res->max_lifetime = max_lifetime;
    res->layout = layout;

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

    settings.ostr << (settings.hilite ? hilite_function : "") << "LIFETIME(";
    settings.ostr << min_lifetime << " " << max_lifetime << ")";
    settings.ostr << (settings.hilite ? hilite_none : "");

    /*
    if (layout)
    {
        settings.ostr << (settings.hilite ? hilite_none : "");
        layout->formatImpl(settings, state, frame);
    }
     */
}

}