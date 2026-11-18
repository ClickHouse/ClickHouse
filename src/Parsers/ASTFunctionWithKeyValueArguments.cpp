#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#include <Poco/String.h>
#include <Common/SipHash.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/maskURIPassword.h>
#include <IO/Operators.h>

namespace DB
{

namespace
{
    /// Keys of a dictionary source whose value must not be shown. Besides the password, this covers
    /// the custom HTTP headers of the `HTTP` source, whose values often carry API tokens. The headers
    /// are hidden as a whole, names included: the query is logged before the dictionary source
    /// validates its structure, so a malformed definition must not leak either.
    bool isSecretKey(const String & key)
    {
        return key == "password" || key == "headers" || key == "header";
    }

    /// Keys whose value is partly secret: the `uri` of the `MongoDB` source, and the `url` or `endpoint` of the `HTTP` source.
    bool isPartlySecretKey(const String & key)
    {
        return key == "uri" || key == "url" || key == "endpoint";
    }

    /// Masks the secret part of the value of a key for which `isPartlySecretKey` holds: the password in the `uri`,
    /// and the password and the sensitive query parameters in the `url` or `endpoint`.
    /// `value` is the formatted value. Returns whether anything was masked.
    bool maskPartlySecretValue(const String & key, String & value)
    {
        if (key == "uri")
            return maskURIPassword(&value);

        /// A value that is no string literal can carry the secret in pieces: hide it whole.
        if (value.size() < 2 || value.front() != '\'' || value.back() != '\'')
        {
            value = "'[HIDDEN]'";
            return true;
        }

        String url = value.substr(1, value.size() - 2);
        bool masked = maskURIPassword(&url);
        String without_parameters = maskSensitiveQueryParametersInURI(url);
        masked |= without_parameters != url;
        if (masked)
            value = "'" + without_parameters + "'";
        return masked;
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


void ASTPair::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << Poco::toUpper(first) << " ";

    if (second_with_brackets)
        ostr << "(";

    if (!settings.show_secrets && isSecretKey(first))
    {
        /// Hide the password and the HTTP headers in the definition of a dictionary:
        /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password '[HIDDEN]' db 'default' table 'ids'))
        ostr << "'[HIDDEN]'";
    }
    else
    {
        String masked;
        if (!settings.show_secrets && isPartlySecretKey(first))
        {
            WriteBufferFromOwnString temp_buf;
            FormatSettings tmp_settings(settings.one_line);
            FormatState tmp_state;
            second->format(temp_buf, tmp_settings, tmp_state, frame);
            masked = temp_buf.str();
            if (!maskPartlySecretValue(first, masked))
                masked.clear();
        }

        if (!masked.empty())
            ostr << masked;
        else
            second->format(ostr, settings, state, frame);
    }

    if (second_with_brackets)
        ostr << ")";
}


bool ASTPair::hasSecretParts() const
{
    if (isSecretKey(first))
        return true;
    if (isPartlySecretKey(first))
    {
        String value = second->formatWithSecretsOneLine();
        if (maskPartlySecretValue(first, value))
            return true;
    }
    return second->hasSecretParts();
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
