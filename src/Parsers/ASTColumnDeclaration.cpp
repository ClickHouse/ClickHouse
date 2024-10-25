#include <Parsers/ASTColumnDeclaration.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTColumnDeclaration::clone() const
{
    const auto res = std::make_shared<ASTColumnDeclaration>(*this);
    res->children.clear();

    if (type)
    {
        res->type = type->clone();
        res->children.push_back(res->type);
    }

    if (default_expression)
    {
        res->default_expression = default_expression->clone();
        res->children.push_back(res->default_expression);
    }

    if (comment)
    {
        res->comment = comment->clone();
        res->children.push_back(res->comment);
    }

    if (codec)
    {
        res->codec = codec->clone();
        res->children.push_back(res->codec);
    }

    if (statistics_desc)
    {
        res->statistics_desc = statistics_desc->clone();
        res->children.push_back(res->statistics_desc);
    }

    if (ttl)
    {
        res->ttl = ttl->clone();
        res->children.push_back(res->ttl);
    }

    if (collation)
    {
        res->collation = collation->clone();
        res->children.push_back(res->collation);
    }

    if (settings)
    {
        res->settings = settings->clone();
        res->children.push_back(res->settings);
    }

    return res;
}

void ASTColumnDeclaration::formatImpl(const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    format_settings.writeIdentifier(name, /*ambiguous=*/true);

    if (type)
    {
        format_settings.ostr << ' ';
        type->formatImpl(format_settings, state, frame);
    }

    if (null_modifier)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "")
                      << (*null_modifier ? "" : "NOT ") << "NULL" << (format_settings.hilite ? hilite_none : "");
    }

    if (default_expression)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << default_specifier << (format_settings.hilite ? hilite_none : "");
        if (!ephemeral_default)
        {
            format_settings.ostr << ' ';
            default_expression->formatImpl(format_settings, state, frame);
        }
    }

    if (comment)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "COMMENT" << (format_settings.hilite ? hilite_none : "") << ' ';
        comment->formatImpl(format_settings, state, frame);
    }

    if (codec)
    {
        format_settings.ostr << ' ';
        codec->formatImpl(format_settings, state, frame);
    }

    if (statistics_desc)
    {
        format_settings.ostr << ' ';
        statistics_desc->formatImpl(format_settings, state, frame);
    }

    if (ttl)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "TTL" << (format_settings.hilite ? hilite_none : "") << ' ';
        ttl->formatImpl(format_settings, state, frame);
    }

    if (collation)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "COLLATE" << (format_settings.hilite ? hilite_none : "") << ' ';
        collation->formatImpl(format_settings, state, frame);
    }

    if (settings)
    {
        format_settings.ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "SETTINGS" << (format_settings.hilite ? hilite_none : "") << ' ' << '(';
        settings->formatImpl(format_settings, state, frame);
        format_settings.ostr << ')';
    }
}

}
