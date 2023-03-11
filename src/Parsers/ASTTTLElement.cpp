#include <Columns/Collator.h>
#include <Common/quoteString.h>
#include <Parsers/ASTTTLElement.h>
#include <IO/Operators.h>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ASTPtr ASTTTLElement::clone() const
{
    auto clone = std::make_shared<ASTTTLElement>(*this);
    clone->children.clear();
    clone->ttl_expr_pos = -1;
    clone->where_expr_pos = -1;

    clone->setExpression(clone->ttl_expr_pos, getExpression(ttl_expr_pos, true));
    clone->setExpression(clone->where_expr_pos, getExpression(where_expr_pos, true));

    for (auto & expr : clone->group_by_key)
        expr = expr->clone();
    for (auto & expr : clone->group_by_assignments)
        expr = expr->clone();

    return clone;
}

void ASTTTLElement::formatImpl(const FormattingBuffer & out) const
{
    ttl()->formatImpl(out);
    if (mode == TTLMode::MOVE)
    {
        if (destination_type == DataDestinationType::DISK)
            out.ostr << " TO DISK ";
        else if (destination_type == DataDestinationType::VOLUME)
            out.ostr << " TO VOLUME ";
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unsupported destination type {} for TTL MOVE",
                    magic_enum::enum_name(destination_type));

        if (if_exists)
            out.ostr << "IF EXISTS ";

        out.ostr << quoteString(destination_name);
    }
    else if (mode == TTLMode::GROUP_BY)
    {
        out.ostr << " GROUP BY ";
        for (const auto * it = group_by_key.begin(); it != group_by_key.end(); ++it)
        {
            if (it != group_by_key.begin())
                out.ostr << ", ";
            (*it)->formatImpl(out);
        }

        if (!group_by_assignments.empty())
        {
            out.ostr << " SET ";
            for (const auto * it = group_by_assignments.begin(); it != group_by_assignments.end(); ++it)
            {
                if (it != group_by_assignments.begin())
                    out.ostr << ", ";
                (*it)->formatImpl(out);
            }
        }
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        out.ostr << " RECOMPRESS ";
        recompression_codec->formatImpl(out);
    }
    else if (mode == TTLMode::DELETE)
    {
        /// It would be better to output "DELETE" here but that will break compatibility with earlier versions.
    }

    if (where())
    {
        out.ostr << " WHERE ";
        where()->formatImpl(out);
    }
}

void ASTTTLElement::setExpression(int & pos, ASTPtr && ast)
{
    if (ast)
    {
        if (pos == -1)
        {
            pos = static_cast<int>(children.size());
            children.emplace_back(ast);
        }
        else
            children[pos] = ast;
    }
    else if (pos != -1)
    {
        children[pos] = ASTPtr{};
        pos = -1;
    }
}

ASTPtr ASTTTLElement::getExpression(int  pos, bool clone) const
{
    return pos != -1 ? (clone ? children[pos]->clone() : children[pos]) : ASTPtr{};
}

}
