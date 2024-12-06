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

void ASTColumnDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    format_settings.writeIdentifier(ostr, name, /*ambiguous=*/true);

    if (type)
    {
        ostr << ' ';
        type->formatImpl(ostr, format_settings, state, frame);
    }

    if (null_modifier)
    {
        ostr << ' ' << (format_settings.hilite ? hilite_keyword : "")
                      << (*null_modifier ? "" : "NOT ") << "NULL" << (format_settings.hilite ? hilite_none : "");
    }

    if (default_expression)
    {
        ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << default_specifier << (format_settings.hilite ? hilite_none : "");
        if (!ephemeral_default)
        {
            ostr << ' ';
            default_expression->formatImpl(ostr, format_settings, state, frame);
        }
    }

    if (comment)
    {
        ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "COMMENT" << (format_settings.hilite ? hilite_none : "") << ' ';
        comment->formatImpl(ostr, format_settings, state, frame);
    }

    if (codec)
    {
        ostr << ' ';
        codec->formatImpl(ostr, format_settings, state, frame);
    }

    if (statistics_desc)
    {
        ostr << ' ';
        statistics_desc->formatImpl(ostr, format_settings, state, frame);
    }

    if (ttl)
    {
        ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "TTL" << (format_settings.hilite ? hilite_none : "") << ' ';
        ttl->formatImpl(ostr, format_settings, state, frame);
    }

    if (collation)
    {
        ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "COLLATE" << (format_settings.hilite ? hilite_none : "") << ' ';
        collation->formatImpl(ostr, format_settings, state, frame);
    }

    if (settings)
    {
        ostr << ' ' << (format_settings.hilite ? hilite_keyword : "") << "SETTINGS" << (format_settings.hilite ? hilite_none : "") << ' ' << '(';
        settings->formatImpl(ostr, format_settings, state, frame);
        ostr << ')';
    }
}

void ASTColumnDeclaration::forEachPointerToChild(std::function<void(void **)> f)
{
    auto visit_child = [&f](ASTPtr & member)
    {
        IAST * new_member_ptr = member.get();
        f(reinterpret_cast<void **>(&new_member_ptr));
        if (new_member_ptr != member.get())
        {
            if (new_member_ptr)
                member = new_member_ptr->ptr();
            else
                member.reset();
        }
    };

    visit_child(default_expression);
    visit_child(comment);
    visit_child(codec);
    visit_child(statistics_desc);
    visit_child(ttl);
    visit_child(collation);
    visit_child(settings);
}
}
