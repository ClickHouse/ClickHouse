#include <Parsers/ASTDictionary.h>
#include <Poco/String.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTDictionaryRange::clone() const
{
    auto res = make_intrusive<ASTDictionaryRange>();
    res->min_attr_name = min_attr_name;
    res->max_attr_name = max_attr_name;
    return res;
}


void ASTDictionaryRange::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DictionaryRange");
    w.writeString("min_attr_name", min_attr_name);
    w.writeString("max_attr_name", max_attr_name);
}

void ASTDictionaryRange::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    min_attr_name = r.getString("min_attr_name");
    max_attr_name = r.getString("max_attr_name");
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
    auto res = make_intrusive<ASTDictionaryLifetime>();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}


void ASTDictionaryLifetime::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DictionaryLifetime");
    w.writeUInt("min_sec", min_sec);
    w.writeUInt("max_sec", max_sec);
}

void ASTDictionaryLifetime::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    min_sec = r.getUInt("min_sec");
    max_sec = r.getUInt("max_sec");
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
    auto res = make_intrusive<ASTDictionaryLayout>();
    res->layout_type = layout_type;
    if (parameters) res->set(res->parameters, parameters->clone());
    res->has_brackets = has_brackets;
    return res;
}


void ASTDictionaryLayout::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DictionaryLayout");
    w.writeString("layout_type", layout_type);
    w.writeBool("has_brackets", has_brackets);
    w.writeChild("parameters", parameters);
}

void ASTDictionaryLayout::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    layout_type = r.getString("layout_type");
    has_brackets = r.getBool("has_brackets");

    auto child = r.readChild("parameters");
    if (child)
        set(parameters, child);
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
    auto res = make_intrusive<ASTDictionarySettings>();
    res->changes = changes;

    return res;
}

void ASTDictionarySettings::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DictionarySettings");
    if (!changes.empty())
    {
        w.writeKey("changes");
        out << '[';
        for (size_t i = 0; i < changes.size(); ++i)
        {
            if (i > 0)
                out << ',';
            out << '{';
            out << "\"name\":";
            writeJSONString(std::string_view(changes[i].name), out, w.getFormatSettings());
            out << ",\"value\":";
            writeJSONString(std::string_view(applyVisitor(FieldVisitorToString(), changes[i].value)), out, w.getFormatSettings());
            out << '}';
        }
        out << ']';
    }
}

void ASTDictionarySettings::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    changes.clear();
    auto arr = r.getArray("changes");
    if (arr)
    {
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto obj = arr->getObject(i);
            if (!obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'changes' array during AST JSON deserialization", i);
            String setting_name = obj->getValue<String>("name");
            String setting_value = obj->getValue<String>("value");
            changes.emplace_back(setting_name, Field(setting_value));
        }
    }
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
    auto res = make_intrusive<ASTDictionary>();

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


void ASTDictionary::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Dictionary");
    w.writeChild("primary_key", primary_key);
    w.writeChild("source", source);
    w.writeChild("lifetime", lifetime);
    w.writeChild("layout", layout);
    w.writeChild("range", range);
    w.writeChild("dict_settings", dict_settings);
}

void ASTDictionary::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    auto child = r.readChild("primary_key");
    if (child)
        set(primary_key, child);

    child = r.readChild("source");
    if (child)
        set(source, child);

    child = r.readChild("lifetime");
    if (child)
        set(lifetime, child);

    child = r.readChild("layout");
    if (child)
        set(layout, child);

    child = r.readChild("range");
    if (child)
        set(range, child);

    child = r.readChild("dict_settings");
    if (child)
        set(dict_settings, child);
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
