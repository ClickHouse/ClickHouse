#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#include <Poco/String.h>

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

    if (second)
    {
        res->second = second;
        res->children.push_back(second);
    }

    return res;
}


void ASTPair::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << Poco::toUpper(first) << " " << (settings.hilite ? hilite_none : "");

    if (second_with_brackets)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "(";

    second->formatImpl(settings, state, frame);

    if (second_with_brackets)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << ")";

    settings.ostr << (settings.hilite ? hilite_none : "");
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
        res->elements->clone();
        res->children.push_back(res->elements);
    }

    return res;
}


void ASTFunctionWithKeyValueArguments::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << Poco::toUpper(name) << (settings.hilite ? hilite_none : "") << (has_brackets ? "(" : "");
    elements->formatImpl(settings, state, frame);
    settings.ostr << (has_brackets ? ")" : "");
    settings.ostr << (settings.hilite ? hilite_none : "");
}

}
