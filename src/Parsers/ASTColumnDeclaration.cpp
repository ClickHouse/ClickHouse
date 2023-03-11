#include <Parsers/ASTColumnDeclaration.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/DataTypeFactory.h>


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

void ASTColumnDeclaration::formatImpl(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);

    /// We have to always backquote column names to avoid ambiguouty with INDEX and other declarations in CREATE query.
    out.ostr << backQuote(name);

    if (type)
    {
        out.ostr << ' ';
        type->formatImpl(out.copyWithIndent(0));
    }

    if (null_modifier)
    {
        out.ostr << ' ';
        out.writeKeyword(*null_modifier ? "" : "NOT ");
        out.writeKeyword("NULL");
    }

    if (default_expression)
    {
        out.ostr << ' ';
        out.writeKeyword(default_specifier);
        if (!ephemeral_default)
        {
            out.ostr << ' ';
            default_expression->formatImpl(out);
        }
    }

    if (comment)
    {
        out.writeKeyword(" COMMENT ");
        comment->formatImpl(out);
    }

    if (codec)
    {
        out.ostr << ' ';
        codec->formatImpl(out);
    }

    if (ttl)
    {
        out.writeKeyword(" TTL ");
        ttl->formatImpl(out);
    }

    if (collation)
    {
        out.writeKeyword(" COLLATE ");
        collation->formatImpl(out);
    }
}

}
