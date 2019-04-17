#include <Parsers/ASTColumnDeclaration.h>


namespace DB
{

ASTPtr ASTColumnDeclaration::clone() const
{
    const auto res = std::make_shared<ASTColumnDeclaration>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type;
        res->children.push_back(res->type);
    }

    if (default_expression)
    {
        res->default_expression = default_expression->clone();
        res->children.push_back(res->default_expression);
    }

    if (codec)
    {
        res->codec = codec->clone();
        res->children.push_back(res->codec);
    }

    if (comment)
    {
        res->comment = comment->clone();
        res->children.push_back(res->comment);
    }

    if (ttl)
    {
        res->ttl = ttl->clone();
        res->children.push_back(res->ttl);
    }

    return res;
}

void ASTColumnDeclaration::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (!settings.one_line)
        settings.ostr << settings.nl_or_ws << std::string(4 * frame.indent, ' ');

    /// We have to always backquote column names to avoid ambiguouty with INDEX and other declarations in CREATE query.
    settings.ostr << backQuote(name);

    if (type)
    {
        settings.ostr << ' ';
        type->formatImpl(settings, state, frame);
    }

    if (default_expression)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << default_specifier << (settings.hilite ? hilite_none : "") << ' ';
        default_expression->formatImpl(settings, state, frame);
    }

    if (comment)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "COMMENT" << (settings.hilite ? hilite_none : "") << ' ';
        comment->formatImpl(settings, state, frame);
    }

    if (codec)
    {
        settings.ostr << ' ';
        codec->formatImpl(settings, state, frame);
    }

    if (ttl)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "TTL" << (settings.hilite ? hilite_none : "") << ' ';
        ttl->formatImpl(settings, state, frame);
    }
}

}
