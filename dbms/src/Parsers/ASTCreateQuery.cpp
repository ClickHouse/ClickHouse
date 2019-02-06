#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST.h>

#include <Core/Types.h>

namespace DB
{

String ASTDictionarySource::getID(char) const
{
    return "Source definition";
}

ASTPtr ASTDictionarySource::clone() const
{
    auto res = std::make_shared<ASTDictionarySource>(*this);
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

void ASTDictionarySource::formatImpl(const FormatSettings & settings,
                                     FormatState & state,
                                     FormatStateStacked frame) const
{
    if (primary_key)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws
                      << "PRIMARY KEY " << (settings.hilite ? hilite_none : "");
        primary_key->formatImpl(settings, state, frame);
    }

    if (source)
    {
        settings.ostr << settings.nl_or_ws;
        source->formatImpl(settings, state, frame);
    }

    if (lifetime)
    {
        settings.ostr << settings.nl_or_ws;
        lifetime->formatImpl(settings, state, frame);
    }

    if (layout)
    {
        settings.ostr << settings.nl_or_ws;
        layout->formatImpl(settings, state, frame);
    }
}

}
