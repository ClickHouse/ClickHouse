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


void ASTDictionaryRange::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword("RANGE");
    out.ostr << "(";
    out.writeKeyword("MIN ");
    out.ostr << min_attr_name << " ";
    out.writeKeyword("MAX ");
    out.ostr << max_attr_name << ")";
}


ASTPtr ASTDictionaryLifetime::clone() const
{
    auto res = std::make_shared<ASTDictionaryLifetime>();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}


void ASTDictionaryLifetime::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword("LIFETIME");
    out.ostr << "(";
    out.writeKeyword("MIN ");
    out.ostr << min_sec << " ";
    out.writeKeyword("MAX ");
    out.ostr << max_sec << ")";
}


ASTPtr ASTDictionaryLayout::clone() const
{
    auto res = std::make_shared<ASTDictionaryLayout>();
    res->layout_type = layout_type;
    if (parameters) res->set(res->parameters, parameters->clone());
    res->has_brackets = has_brackets;
    return res;
}


void ASTDictionaryLayout::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword("LAYOUT");
    out.ostr << "(";
    out.writeKeyword(Poco::toUpper(layout_type));

    if (has_brackets)
        out.ostr << "(";

    if (parameters)
        parameters->formatImpl(out);

    if (has_brackets)
        out.ostr << ")";

    out.ostr << ")";
}

ASTPtr ASTDictionarySettings::clone() const
{
    auto res = std::make_shared<ASTDictionarySettings>();
    res->changes = changes;

    return res;
}

void ASTDictionarySettings::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword("SETTINGS");
    out.ostr << "(";
    for (auto it = changes.begin(); it != changes.end(); ++it)
    {
        if (it != changes.begin())
            out.ostr << ", ";

        out.ostr << it->name << " = " << applyVisitor(FieldVisitorToString(), it->value);
    }
    out.ostr << ")";
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


void ASTDictionary::formatImpl(FormattingBuffer out) const
{
    if (primary_key)
    {
        out.nlOrWs();
        out.writeKeyword("PRIMARY KEY ");
        primary_key->formatImpl(out);
    }

    if (source)
    {
        out.nlOrWs();
        out.writeKeyword("SOURCE");
        out.ostr << "(";
        source->formatImpl(out);
        out.ostr << ")";
    }

    if (lifetime)
    {
        out.nlOrWs();
        lifetime->formatImpl(out);
    }

    if (layout)
    {
        out.nlOrWs();
        layout->formatImpl(out);
    }

    if (range)
    {
        out.nlOrWs();
        range->formatImpl(out);
    }

    if (dict_settings)
    {
        out.nlOrWs();
        dict_settings->formatImpl(out);
    }
}

}
