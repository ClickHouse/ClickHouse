#include <Parsers/ASTDictionary.h>
#include <Poco/String.h>
#include <IO/Operators.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{

ASTPtr ASTDictionaryRange::clone() const
{
    auto res = std::make_shared<ASTDictionaryRange>();
    res->min_attr_name = min_attr_name;
    res->max_attr_name = max_attr_name;
    return res;
}


void ASTDictionaryRange::formatImpl(const FormatSettings & settings,
                                    FormatState &,
                                    FormatStateStacked) const
{
    settings.writeKeyword("RANGE");
    settings.ostr << "(";
    settings.writeKeyword("MIN ");
    settings.ostr << min_attr_name << " ";
    settings.writeKeyword("MAX ");
    settings.ostr << max_attr_name << ")";
}


ASTPtr ASTDictionaryLifetime::clone() const
{
    auto res = std::make_shared<ASTDictionaryLifetime>();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}


void ASTDictionaryLifetime::formatImpl(const FormatSettings & settings,
                                       FormatState &,
                                       FormatStateStacked) const
{
    settings.writeKeyword("LIFETIME");
    settings.ostr << "(";
    settings.writeKeyword("MIN ");
    settings.ostr << min_sec << " ";
    settings.writeKeyword("MAX ");
    settings.ostr << max_sec << ")";
}


ASTPtr ASTDictionaryLayout::clone() const
{
    auto res = std::make_shared<ASTDictionaryLayout>();
    res->layout_type = layout_type;
    if (parameters) res->set(res->parameters, parameters->clone());
    res->has_brackets = has_brackets;
    return res;
}


void ASTDictionaryLayout::formatImpl(const FormatSettings & settings,
                                     FormatState & state,
                                     FormatStateStacked frame) const
{
    settings.writeKeyword("LAYOUT");
    settings.ostr << "(";
    settings.writeKeyword(Poco::toUpper(layout_type));

    if (has_brackets)
        settings.ostr << "(";

    if (parameters)
        parameters->formatImpl(settings);

    if (has_brackets)
        settings.ostr << ")";

    settings.ostr << ")";
}

ASTPtr ASTDictionarySettings::clone() const
{
    auto res = std::make_shared<ASTDictionarySettings>();
    res->changes = changes;

    return res;
}

void ASTDictionarySettings::formatImpl(const FormatSettings & settings,
                                        FormatState &,
                                        FormatStateStacked) const
{
    settings.writeKeyword("SETTINGS");
    settings.ostr << "(";
    for (auto it = changes.begin(); it != changes.end(); ++it)
    {
        if (it != changes.begin())
            settings.ostr << ", ";

        settings.ostr << it->name << " = " << applyVisitor(FieldVisitorToString(), it->value);
    }
    settings.ostr << ")";
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


void ASTDictionary::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (primary_key)
    {
        settings.nlOrWs();
        settings.writeKeyword("PRIMARY KEY ");
        primary_key->formatImpl(settings);
    }

    if (source)
    {
        settings.nlOrWs();
        settings.writeKeyword("SOURCE(");
        source->formatImpl(settings, state, frame);
        settings.writeKeyword(")");
    }

    if (lifetime)
    {
        settings.nlOrWs();
        lifetime->formatImpl(settings, state, frame);
    }

    if (layout)
    {
        settings.nlOrWs();
        layout->formatImpl(settings, state, frame);
    }

    if (range)
    {
        settings.nlOrWs();
        range->formatImpl(settings, state, frame);
    }

    if (dict_settings)
    {
        settings.nlOrWs();
        dict_settings->formatImpl(settings, state, frame);
    }
}

}
