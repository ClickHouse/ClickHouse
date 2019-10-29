#include <Parsers/ASTDictionary.h>
#include <Poco/String.h>

namespace DB
{

ASTPtr ASTDictionaryRange::clone() const
{
    auto res = std::make_shared<ASTDictionaryRange>(*this);
    res->children.clear();
    res->min_attr_name = min_attr_name;
    res->max_attr_name = max_attr_name;
    return res;
}


void ASTDictionaryRange::formatImpl(const FormatSettings & settings,
                                    FormatState &,
                                    FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "RANGE"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << "MIN "
                  << min_attr_name << " "
                  << (settings.hilite ? hilite_keyword : "")
                  << "MAX "
                  << (settings.hilite ? hilite_none : "")
                  << max_attr_name << ")";
}



ASTPtr ASTDictionaryLifetime::clone() const
{
    auto res = std::make_shared<ASTDictionaryLifetime>(*this);
    res->children.clear();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}


void ASTDictionaryLifetime::formatImpl(const FormatSettings & settings,
                                       FormatState &,
                                       FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "LIFETIME"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << "MIN "
                  << min_sec << " "
                  << (settings.hilite ? hilite_keyword : "")
                  << "MAX "
                  << (settings.hilite ? hilite_none : "")
                  << max_sec << ")";
}


ASTPtr ASTDictionaryLayout::clone() const
{
    auto res = std::make_shared<ASTDictionaryLayout>(*this);
    res->children.clear();
    res->layout_type = layout_type;
    if (parameter.has_value())
    {
        res->parameter.emplace(parameter->first, nullptr);
        res->set(res->parameter->second, parameter->second->clone());
    }
    return res;
}


void ASTDictionaryLayout::formatImpl(const FormatSettings & settings,
                                     FormatState & state,
                                     FormatStateStacked expected) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "LAYOUT"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << Poco::toUpper(layout_type)
                  << (settings.hilite ? hilite_none : "");

    settings.ostr << "(";
    if (parameter)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << Poco::toUpper(parameter->first)
                      << (settings.hilite ? hilite_none : "")
                      << " ";

        parameter->second->formatImpl(settings, state, expected);
    }
    settings.ostr << ")";
    settings.ostr << ")";
}


ASTPtr ASTDictionary::clone() const
{
    auto res = std::make_shared<ASTDictionary>(*this);
    res->children.clear();

    if (source)
        res->set(res->source, source->clone());

    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    if (lifetime)
        res->set(res->lifetime, lifetime->clone());

    if (layout)
        res->set(res->layout, layout->clone());

    if (range)
        res->set(res->range, range->clone());

    return res;
}


void ASTDictionary::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (primary_key)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "PRIMARY KEY "
            << (settings.hilite ? hilite_none : "");
        primary_key->formatImpl(settings, state, frame);
    }

    if (source)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "SOURCE("
            << (settings.hilite ? hilite_none : "");
        source->formatImpl(settings, state, frame);
        settings.ostr << ")";
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

    if (range)
    {
        settings.ostr << settings.nl_or_ws;
        range->formatImpl(settings, state, frame);
    }
}

}
