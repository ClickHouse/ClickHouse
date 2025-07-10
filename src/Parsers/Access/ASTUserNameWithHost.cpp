#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ASTUserNameWithHost::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << backQuoteIfNeed(getBaseName());
    if (auto pattern = getHostPattern(); !pattern.empty())
        ostr << "@" << backQuoteIfNeed(pattern);
}

String ASTUserNameWithHost::toString() const
{
    WriteBufferFromOwnString ostr;

    ostr << getBaseName();
    if (auto pattern = getHostPattern(); !pattern.empty())
        ostr << "@" << pattern;

    return ostr.str();
}

void ASTUserNameWithHost::replace(const String name)
{
    children.clear();

    username.reset();
    host_pattern.reset();

    username = std::make_shared<ASTIdentifier>(name);
    children.emplace_back(username);
}

ASTUserNameWithHost::ASTUserNameWithHost(const String & name)
{
    username = std::make_shared<ASTIdentifier>(name);
    children.emplace_back(username);
}

ASTUserNameWithHost::ASTUserNameWithHost(ASTPtr && name_, String && host_pattern_)
{
    username = std::move(name_);
    children.emplace_back(username);

    if (!host_pattern_.empty() && host_pattern_ != "%")
    {
        host_pattern = std::make_shared<ASTLiteral>(std::move(host_pattern_));
        children.emplace_back(host_pattern);
    }
}

String ASTUserNameWithHost::getBaseName() const
{
    if (children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTUserNameWithHost is empty");

    chassert(username);

    return getStringFromAST(username);
}

String ASTUserNameWithHost::getHostPattern() const
{
    if (children.size() < 2)
        return "";

    chassert(children.size() == 2);
    chassert(host_pattern);

    return getStringFromAST(host_pattern);
}


ASTUserNamesWithHost::ASTUserNamesWithHost(const String & name_)
{
    children.emplace_back(std::make_shared<ASTUserNameWithHost>(name_));
}

void ASTUserNamesWithHost::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    assert(!children.empty());

    bool need_comma = false;
    for (const auto & child : children)
    {
        if (std::exchange(need_comma, true))
            ostr << ", ";
        child->format(ostr, settings, state, frame);
    }
}

Strings ASTUserNamesWithHost::toStrings() const
{
    Strings res;
    res.reserve(children.size());
    for (const auto & name : children)
    {
        const auto & name_ast = name->as<const ASTUserNameWithHost &>();
        res.emplace_back(name_ast.toString());
    }
    return res;
}

bool ASTUserNamesWithHost::getHostPatternIfCommon(String & out_common_host_pattern) const
{
    out_common_host_pattern.clear();

    if (children.empty())
        return true;

    for (size_t i = 1; i != children.size(); ++i)
        if (children[i]->as<const ASTUserNameWithHost &>().getHostPattern()
            != children[0]->as<const ASTUserNameWithHost &>().getHostPattern())
            return false;

    out_common_host_pattern = children[0]->as<const ASTUserNameWithHost &>().getHostPattern();
    return true;
}

String ASTUserNameWithHost::getStringFromAST(const ASTPtr & ast) const
{
    if (const auto * literal = ast->as<ASTLiteral>())
        return literal->value.safeGet<String>();
    else if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        if (!identifier->isParam())
            return getIdentifierName(identifier);

        WriteBufferFromOwnString buf;
        FormatSettings settings(true, false);

        identifier->format(buf, settings);
        return buf.str();
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unsupported type '{}' in ASTUserNameWithHost for formatting to String", ast ? ast->getID() : "NULL");
}

ASTPtr ASTUserNameWithHost::clone() const
{
    auto clone = std::make_shared<ASTUserNameWithHost>(*this);
    clone->children.clear();

    clone->username = username->clone();
    clone->children.emplace_back(clone->username);

    if (host_pattern)
    {
        clone->host_pattern = host_pattern->clone();
        clone->children.emplace_back(clone->host_pattern);
    }

    return clone;
}

}
