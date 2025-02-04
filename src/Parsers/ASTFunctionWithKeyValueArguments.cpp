#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#include <Poco/String.h>
#include <Common/SipHash.h>
#include <Common/maskURIPassword.h>
#include <IO/Operators.h>

namespace DB
{

String ASTPair::getID(char) const
{
    return "pair";
}


ASTPtr ASTPair::clone() const
{
    auto res = std::make_shared<ASTPair>(*this);
    res->children.clear();
    res->set(res->second, second->clone());
    return res;
}


void ASTPair::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << Poco::toUpper(first) << " " << (settings.hilite ? hilite_none : "");

    if (second_with_brackets)
        ostr << (settings.hilite ? hilite_keyword : "") << "(";

    if (!settings.show_secrets && (first == "password"))
    {
        /// Hide password in the definition of a dictionary:
        /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password '[HIDDEN]' db 'default' table 'ids'))
        ostr << "'[HIDDEN]'";
    }
    else if (!settings.show_secrets && (first == "uri"))
    {
        // Hide password from URI in the defention of a dictionary
        WriteBufferFromOwnString temp_buf;
        FormatSettings tmp_settings(settings.one_line);
        FormatState tmp_state;
        second->formatImpl(temp_buf, tmp_settings, tmp_state, frame);

        maskURIPassword(&temp_buf.str());
        ostr << temp_buf.str();
    }
    else
    {
        second->formatImpl(ostr, settings, state, frame);
    }

    if (second_with_brackets)
        ostr << (settings.hilite ? hilite_keyword : "") << ")";

    ostr << (settings.hilite ? hilite_none : "");
}


bool ASTPair::hasSecretParts() const
{
    return first == "password";
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
    auto res = std::make_shared<ASTFunctionWithKeyValueArguments>(*this);
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
    ostr << (settings.hilite ? hilite_keyword : "") << Poco::toUpper(name) << (settings.hilite ? hilite_none : "") << (has_brackets ? "(" : "");
    elements->formatImpl(ostr, settings, state, frame);
    ostr << (has_brackets ? ")" : "");
    ostr << (settings.hilite ? hilite_none : "");
}


void ASTFunctionWithKeyValueArguments::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(has_brackets);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
