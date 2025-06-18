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
    if (auto host_pattern = getHostPattern(); !host_pattern.empty())
        ostr << "@" << backQuoteIfNeed(host_pattern);
}

String ASTUserNameWithHost::toString() const
{
    WriteBufferFromOwnString ostr;

    ostr << getBaseName();
    if (auto host_pattern = getHostPattern(); !host_pattern.empty())
        ostr << "@" << host_pattern;

    return ostr.str();
}

void ASTUserNameWithHost::replace(const String name_)
{
    children.clear();
    children.emplace_back(std::make_shared<ASTIdentifier>(name_));
}


ASTUserNameWithHost::ASTUserNameWithHost(const String & name_)
{
    children.emplace_back(std::make_shared<ASTIdentifier>(name_));
}

ASTUserNameWithHost::ASTUserNameWithHost(ASTPtr && name_ast_, String host_pattern_)
{
    children.emplace_back(std::move(name_ast_));
    if (!host_pattern_.empty() && host_pattern_ != "%")
        children.emplace_back(std::make_shared<ASTLiteral>(host_pattern_));
}

String ASTUserNameWithHost::getBaseName() const
{
    if (children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ASTUserNameWithHost is empty");

    return getStringFromAST(children[0]);
}

String ASTUserNameWithHost::getHostPattern() const
{
    if (children.size() < 2)
        return "";
    chassert(children.size() == 2);
    return getStringFromAST(children[1]);
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
    if (const auto * l = ast->as<ASTLiteral>())
        return l->value.safeGet<String>();
    else if (const auto * ind = ast->as<ASTIdentifier>())
    {
        if (!ind->isParam())
            return getIdentifierName(ind);

        WriteBufferFromOwnString buf;
        FormatSettings settings(true, false);

        ind->format(buf, settings);
        return buf.str();
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unsupported type '{}' in ASTUserNameWithHost for formatting to String", ast ? ast->getID() : "NULL");
}
}
