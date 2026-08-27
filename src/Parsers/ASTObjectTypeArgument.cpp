#include <IO/Operators.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/CommonParsers.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

ASTPtr ASTObjectTypedPathArgument::clone() const
{
    auto res = std::make_shared<ASTObjectTypedPathArgument>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    return res;
}

void ASTObjectTypedPathArgument::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    /// We must quote path "SKIP" to avoid its confusion with SKIP keyword in Object arguments.
    if (boost::to_upper_copy(path) == "SKIP")
        ostr << backQuote(path) << ' ';
    else
        ostr << backQuoteIfNeed(path) << ' ';

    type->format(ostr, settings, state, frame);
}

ASTPtr ASTObjectTypeArgument::clone() const
{
    auto res = std::make_shared<ASTObjectTypeArgument>(*this);
    res->children.clear();

    if (path_with_type)
    {
        res->path_with_type = path_with_type->clone();
        res->children.push_back(res->path_with_type);
    }
    else if (skip_path)
    {
        res->skip_path = skip_path->clone();
        res->children.push_back(res->skip_path);
    }
    else if (skip_path_regexp)
    {
        res->skip_path_regexp = skip_path_regexp->clone();
        res->children.push_back(res->skip_path_regexp);
    }
    else if (parameter)
    {
        res->parameter = parameter->clone();
        res->children.push_back(res->parameter);
    }

    return res;
}

void ASTObjectTypeArgument::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (path_with_type)
    {
        path_with_type->format(ostr, settings, state, frame);
    }
    else if (parameter)
    {
        parameter->format(ostr, settings, state, frame);
    }
    else if (skip_path)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "SKIP" << ' ';
        skip_path->format(ostr, settings, state, frame);
    }
    else if (skip_path_regexp)
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << indent_str << "SKIP REGEXP" << ' ';
        skip_path_regexp->format(ostr, settings, state, frame);
    }
}

}


