#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/String.h>
#include <Common/SipHash.h>
#include <Common/maskURIPassword.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// Keys of a dictionary source whose value must not be shown. Besides the password, this covers
    /// the TLS credentials that are given as the contents of a certificate or a key file (a path is
    /// not accepted from a `CREATE DICTIONARY` query in the first place).
    bool isSecretKey(const String & key)
    {
        return key == "password"
            || key == "ssl_ca_pem" || key == "ssl_cert_pem" || key == "ssl_key_pem"
            || key == "sslrootcert_pem" || key == "sslcert_pem" || key == "sslkey_pem";
    }
}

String ASTPair::getID(char) const
{
    return "pair";
}


ASTPtr ASTPair::clone() const
{
    auto res = make_intrusive<ASTPair>(*this);
    res->children.clear();
    res->set(res->second, second->clone());
    return res;
}


void ASTPair::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Pair");
    w.writeString("first", first);
    w.writeBool("second_with_brackets", second_with_brackets);
    w.writeChild("second", second);
}

void ASTPair::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    /// The SQL parser lower-cases the key (see `ParserKeyValuePair`), and the checks for secret keys in
    /// `formatImpl` and `hasSecretParts` rely on it, so canonicalize it the same way here.
    first = Poco::toLower(r.getString("first"));
    if (first.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or empty 'first' in ASTPair during AST JSON deserialization");

    second_with_brackets = r.getBool("second_with_brackets");

    auto child = r.readChild("second");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'second' in ASTPair during AST JSON deserialization");

    /// A value in brackets is parser-produced as an `ASTExpressionList` of `ASTPair`, e.g. `headers(header(...))`.
    /// `formatImpl` relies on this shape to hide secrets, so malformed `clickhouse_json` fails with `BAD_ARGUMENTS`.
    if (second_with_brackets)
    {
        const auto * list = child->as<ASTExpressionList>();
        if (!list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'second' of ASTPair in brackets must be a list of key-value pairs during AST JSON deserialization");
        for (const auto & element : list->children)
            if (!element || !element->as<ASTPair>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "'second' of ASTPair in brackets must contain only key-value pairs during AST JSON deserialization");
    }

    set(second, child);
}

void ASTPair::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << Poco::toUpper(first) << " ";

    if (second_with_brackets)
        ostr << "(";

    if (!settings.show_secrets && isSecretKey(first))
    {
        /// Hide the password and the TLS credentials in the definition of a dictionary:
        /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password '[HIDDEN]' db 'default' table 'ids'))
        ostr << "'[HIDDEN]'";
    }
    else if (!settings.show_secrets && (first == "uri"))
    {
        // Hide password from URI in the defention of a dictionary
        WriteBufferFromOwnString temp_buf;
        FormatSettings tmp_settings(settings.one_line);
        FormatState tmp_state;
        second->format(temp_buf, tmp_settings, tmp_state, frame);

        maskURIPassword(&temp_buf.str());
        ostr << temp_buf.str();
    }
    else if (!settings.show_secrets && (first == "headers" || first == "header"))
    {
        /// Hide the values of HTTP headers in the definition of a dictionary, keeping their names.
        /// They often carry credentials (e.g. API tokens), so all of them are hidden, the same way
        /// as the `url` table function hides header values:
        /// SOURCE(HTTP(url 'http://example.com/' format 'TSV' headers(header(name 'API-KEY' value '[HIDDEN]'))))
        /// The query is logged before the dictionary source rejects unknown keys, so a malformed
        /// definition must not leak either: inside `headers` only `header(...)` entries are kept
        /// (they hide their own values when formatted), inside `header` only a `name` that is a literal or
        /// an identifier is kept (a function is rejected only later, after the query is logged).
        /// Anything but a list of pairs is hidden as a whole, whatever produced the AST.
        bool hide_all = !second_with_brackets || !second->as<ASTExpressionList>();
        ASTPtr masked;
        if (!hide_all)
        {
            masked = second->clone();
            for (auto & child : masked->children)
            {
                auto * pair = child->as<ASTPair>();
                if (!pair)
                {
                    hide_all = true;
                    break;
                }

                bool keep = first == "headers"
                    ? pair->first == "header" && pair->second_with_brackets
                    : pair->first == "name" && !pair->second_with_brackets
                        && (pair->second->as<ASTLiteral>() || pair->second->as<ASTIdentifier>());
                if (!keep)
                {
                    pair->second_with_brackets = false;
                    pair->replace(pair->second, make_intrusive<ASTLiteral>("[HIDDEN]"));
                }
            }
        }

        if (hide_all)
            ostr << "'[HIDDEN]'";
        else
            masked->format(ostr, settings, state, frame);
    }
    else
    {
        second->format(ostr, settings, state, frame);
    }

    if (second_with_brackets)
        ostr << ")";
}


bool ASTPair::hasSecretParts() const
{
    /// `headers` is checked too, not only `header`: a malformed `headers(...)` without any `header(...)`
    /// entry is still masked when formatted and must be masked in the query logs as well.
    return isSecretKey(first) || first == "headers" || first == "header" || second->hasSecretParts();
}


void ASTPair::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(first.size());
    hash_state.update(first);
    hash_state.update(second_with_brackets);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


String ASTFunctionWithKeyValueArguments::getID(char delim) const
{
    return "FunctionWithKeyValueArguments " + (delim + name);
}


ASTPtr ASTFunctionWithKeyValueArguments::clone() const
{
    auto res = make_intrusive<ASTFunctionWithKeyValueArguments>(*this);
    res->children.clear();

    if (elements)
    {
        res->elements = elements->clone();
        res->children.push_back(res->elements);
    }

    return res;
}


void ASTFunctionWithKeyValueArguments::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "FunctionWithKeyValueArguments");
    w.writeString("name", name);
    w.writeBool("has_brackets", has_brackets);
    w.writeChild("elements", elements);
}

void ASTFunctionWithKeyValueArguments::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or empty 'name' in ASTFunctionWithKeyValueArguments during AST JSON deserialization");

    has_brackets = r.getBool("has_brackets");

    /// `elements` is parser-produced as an `ASTExpressionList` of `ASTPair`;
    /// `buildConfigurationFromFunctionWithKeyValueArguments` does `elements->as<const ASTExpressionList>()`
    /// and dereferences each child as an `ASTPair`. Validate both layers so malformed dictionary
    /// `clickhouse_json` fails with `BAD_ARGUMENTS` instead of inside dictionary-configuration building.
    elements = r.readChildOfType<ASTExpressionList>("elements");
    if (!elements)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'elements' in ASTFunctionWithKeyValueArguments during AST JSON deserialization");
    for (const auto & element : elements->children)
        if (!element || !element->as<ASTPair>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'elements' of ASTFunctionWithKeyValueArguments must contain only key-value pairs during AST JSON deserialization");
    children.push_back(elements);
}

void ASTFunctionWithKeyValueArguments::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << Poco::toUpper(name) << (has_brackets ? "(" : "");
    elements->format(ostr, settings, state, frame);
    ostr << (has_brackets ? ")" : "");
}


void ASTFunctionWithKeyValueArguments::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(has_brackets);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
