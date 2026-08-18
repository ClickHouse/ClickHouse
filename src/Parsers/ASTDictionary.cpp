#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
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

    /// `ParserDictionaryRange` requires both `MIN` and `MAX` to be non-empty identifiers, and
    /// `formatImpl` prints both unconditionally, so a missing or empty name would format a
    /// parser-impossible `RANGE(...)` clause instead of failing at the JSON boundary.
    if (!r.has("min_attr_name"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'min_attr_name' in `DictionaryRange` during AST JSON deserialization");
    if (!r.has("max_attr_name"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'max_attr_name' in `DictionaryRange` during AST JSON deserialization");

    min_attr_name = r.getString("min_attr_name");
    max_attr_name = r.getString("max_attr_name");

    if (min_attr_name.empty() || max_attr_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty attribute name in `DictionaryRange` during AST JSON deserialization");
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

    /// Both bounds are printed unconditionally by `formatImpl`, so a missing key would silently
    /// become `LIFETIME(MIN 0 MAX 0)` — a different, valid dictionary definition. `writeJSON`
    /// always emits both; require them.
    if (!r.has("min_sec"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'min_sec' in `DictionaryLifetime` during AST JSON deserialization");
    if (!r.has("max_sec"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'max_sec' in `DictionaryLifetime` during AST JSON deserialization");

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

    /// `ParserDictionaryLayout` takes the layout name from an identifier, so it is never empty, and
    /// `formatImpl` prints it unconditionally: a missing name would format a parser-impossible
    /// `LAYOUT()`.
    if (!r.has("layout_type"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'layout_type' in `DictionaryLayout` during AST JSON deserialization");
    layout_type = r.getString("layout_type");
    if (layout_type.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'layout_type' in `DictionaryLayout` during AST JSON deserialization");

    has_brackets = r.getBool("has_brackets");

    auto child = r.readChildOfType<ASTExpressionList>("parameters");
    if (child)
    {
        /// The parser rejects layout parameters without brackets, and `formatImpl` prints the
        /// parameters outside the brackets in that case (`LAYOUT(CACHE SIZE_IN_CELLS 10)`).
        if (!child->children.empty() && !has_brackets)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`DictionaryLayout` parameters require brackets during AST JSON deserialization");
        set(parameters, child);
    }
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
        auto & o = w.getOut();
        const auto & fs = w.getFormatSettings();
        o << '[';
        for (size_t i = 0; i < changes.size(); ++i)
        {
            if (i > 0)
                o << ',';
            o << "{\"name\":";
            writeJSONString(changes[i].name, o, fs);
            w.writeFieldValue("value", changes[i].value);
            o << '}';
        }
        o << ']';
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
            /// Read the name strictly so a non-string value is rejected with `BAD_ARGUMENTS`
            /// instead of being coerced into a setting name.
            JSONObjectReader setting_reader(*obj);
            String setting_name = setting_reader.getString("name");
            auto value_obj = obj->getObject("value");
            if (!value_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'value' object at index {} in 'changes' array during AST JSON deserialization", i);
            changes.emplace_back(setting_name, JSONObjectReader::readFieldFromObject(*value_obj));
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

    /// All these slots are concrete typed members; restoring them through the generic child path would
    /// let a wrong node type reach `IAST::set` as a `LOGICAL_ERROR` cast failure instead of a
    /// user-facing `BAD_ARGUMENTS`. Restore each with `readChildOfType`.
    auto child = r.readChildOfType<ASTExpressionList>("primary_key");
    if (child)
        set(primary_key, child);

    child = r.readChildOfType<ASTFunctionWithKeyValueArguments>("source");
    if (child)
        set(source, child);

    child = r.readChildOfType<ASTDictionaryLifetime>("lifetime");
    if (child)
        set(lifetime, child);

    child = r.readChildOfType<ASTDictionaryLayout>("layout");
    if (child)
        set(layout, child);

    child = r.readChildOfType<ASTDictionaryRange>("range");
    if (child)
        set(range, child);

    child = r.readChildOfType<ASTDictionarySettings>("dict_settings");
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
