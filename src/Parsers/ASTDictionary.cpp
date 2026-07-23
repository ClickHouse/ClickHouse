#include <Parsers/ASTDictionary.h>
#include <Poco/String.h>
#include <IO/Operators.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>


namespace DB
{

ASTPtr ASTDictionaryRange::clone() const
{
    auto res = std::make_shared<ASTDictionaryRange>();
    res->min_attr_name = min_attr_name;
    res->max_attr_name = max_attr_name;
    return res;
}


void ASTDictionaryRange::formatImpl(WriteBuffer & ostr,
                                    const FormatSettings &,
                                    FormatState &,
                                    FormatStateStacked) const
{
    ostr << "RANGE(MIN " << backQuoteIfNeed(min_attr_name) << " MAX " << backQuoteIfNeed(max_attr_name) << ")";
}


ASTPtr ASTDictionaryLifetime::clone() const
{
    auto res = std::make_shared<ASTDictionaryLifetime>();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}


void ASTDictionaryLifetime::formatImpl(WriteBuffer & ostr,
                                       const FormatSettings &,
                                       FormatState &,
                                       FormatStateStacked) const
{
    ostr << "LIFETIME(MIN " << min_sec << " MAX " << max_sec << ")";
}


ASTPtr ASTDictionaryLayout::clone() const
{
    auto res = std::make_shared<ASTDictionaryLayout>();
    res->layout_type = layout_type;
    if (parameters) res->set(res->parameters, parameters->clone());
    res->has_brackets = has_brackets;
    return res;
}


void ASTDictionaryLayout::formatImpl(WriteBuffer & ostr,
                                     const FormatSettings & settings,
                                     FormatState & state,
                                     FormatStateStacked frame) const
{
    ostr << "LAYOUT(" << Poco::toUpper(layout_type);

    if (has_brackets)
        ostr << "(";

    if (parameters)
        parameters->format(ostr, settings, state, frame);

    if (has_brackets)
        ostr << ")";

    ostr << ")";
}

ASTPtr ASTDictionarySettings::clone() const
{
    auto res = std::make_shared<ASTDictionarySettings>();
    res->changes = changes;

    return res;
}

void ASTDictionarySettings::formatImpl(WriteBuffer & ostr,
                                       const FormatSettings &,
                                       FormatState &,
                                       FormatStateStacked) const
{

    ostr << "SETTINGS(";
    for (auto it = changes.begin(); it != changes.end(); ++it)
    {
        if (it != changes.begin())
            ostr << ", ";

        ostr << it->name << " = " << applyVisitor(FieldVisitorToString(), it->value);
    }
    ostr << ")";
}


ASTPtr ASTDictionary::clone() const
{
    auto res = std::make_shared<ASTDictionary>();

    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    if (source)
        res->set(res->source, source->clone());

    if (lifetime)
        res->set(res->lifetime, lifetime->clone());

    if (layout)
        res->set(res->layout, layout->clone());

    if (range)
        res->set(res->range, range->clone());

    if (dict_settings)
        res->set(res->dict_settings, dict_settings->clone());

    return res;
}


void ASTDictionary::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (primary_key)
    {
        ostr << settings.nl_or_ws << "PRIMARY KEY ";
        primary_key->format(ostr, settings, state, frame);
    }

    if (source)
    {
        ostr << settings.nl_or_ws << "SOURCE";
        ostr << "(";
        source->format(ostr, settings, state, frame);
        ostr << ")";
    }

    if (lifetime)
    {
        ostr << settings.nl_or_ws;
        lifetime->format(ostr, settings, state, frame);
    }

    if (layout)
    {
        ostr << settings.nl_or_ws;
        layout->format(ostr, settings, state, frame);
    }

    if (range)
    {
        ostr << settings.nl_or_ws;
        range->format(ostr, settings, state, frame);
    }

    if (dict_settings)
    {
        ostr << settings.nl_or_ws;
        dict_settings->format(ostr, settings, state, frame);
    }
}

}
