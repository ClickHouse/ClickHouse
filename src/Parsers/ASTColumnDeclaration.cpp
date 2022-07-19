#include <Parsers/ASTColumnDeclaration.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

ASTPtr ASTColumnDeclaration::clone() const
{
    const auto res = std::make_shared<ASTColumnDeclaration>(*this);
    res->children.clear();

    if (type)
    {
        // Type may be an ASTFunction (e.g. `create table t (a Decimal(9,0))`),
        // so we have to clone it properly as well.
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

    return res;
}

void ASTColumnDeclaration::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    /// We have to always backquote column names to avoid ambiguouty with INDEX and other declarations in CREATE query.
    settings.ostr << backQuote(name);

    if (type)
    {
        settings.ostr << ' ';

        FormatStateStacked type_frame = frame;
        type_frame.indent = 0;

        type->formatImpl(settings, state, type_frame);
    }

    if (null_modifier)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "")
                      << (*null_modifier ? "" : "NOT ") << "NULL" << (settings.hilite ? hilite_none : "");
    }

    if (default_expression)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << default_specifier << (settings.hilite ? hilite_none : "");
        if (default_specifier != "EPHEMERAL" || !default_expression->as<ASTLiteral>()->value.isNull())
        {
            settings.ostr << ' ';
            default_expression->formatImpl(settings, state, frame);
        }
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

    if (collation)
    {
        settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << "COLLATE" << (settings.hilite ? hilite_none : "") << ' ';
        collation->formatImpl(settings, state, frame);
    }
}

}
