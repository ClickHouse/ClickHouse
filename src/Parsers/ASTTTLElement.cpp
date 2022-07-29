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
    clone->ttl_expr_pos = clone->children.end();
    clone->where_expr_pos = clone->children.end();

    clone->setExpression(clone->ttl_expr_pos, getExpression(ttl_expr_pos, true));
    clone->setExpression(clone->where_expr_pos, getExpression(where_expr_pos, true));

    for (auto & expr : clone->group_by_key)
        expr = expr->clone();
    for (auto & expr : clone->group_by_assignments)
        expr = expr->clone();

    return clone;
}

void ASTTTLElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ttl()->formatImpl(settings, state, frame);
    if (mode == TTLMode::MOVE)
    {
        if (destination_type == DataDestinationType::DISK)
            settings.ostr << " TO DISK ";
        else if (destination_type == DataDestinationType::VOLUME)
            settings.ostr << " TO VOLUME ";
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unsupported destination type {} for TTL MOVE",
                    magic_enum::enum_name(destination_type));

        if (if_exists)
            settings.ostr << "IF EXISTS ";

        settings.ostr << quoteString(destination_name);
    }
    else if (mode == TTLMode::GROUP_BY)
    {
        settings.ostr << " GROUP BY ";
        for (auto it = group_by_key.begin(); it != group_by_key.end(); ++it)
        {
            if (it != group_by_key.begin())
                settings.ostr << ", ";
            (*it)->formatImpl(settings, state, frame);
        }

        if (!group_by_assignments.empty())
        {
            settings.ostr << " SET ";
            for (auto it = group_by_assignments.begin(); it != group_by_assignments.end(); ++it)
            {
                if (it != group_by_assignments.begin())
                    settings.ostr << ", ";
                (*it)->formatImpl(settings, state, frame);
            }
        }
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        settings.ostr << " RECOMPRESS ";
        recompression_codec->formatImpl(settings, state, frame);
    }
    else if (mode == TTLMode::DELETE)
    {
        /// It would be better to output "DELETE" here but that will break compatibility with earlier versions.
    }

    if (where())
    {
        settings.ostr << " WHERE ";
        where()->formatImpl(settings, state, frame);
    }
}

void ASTTTLElement::setExpression(ASTList::iterator & pos, ASTPtr && ast)
{
    if (ast)
    {
        if (pos == children.end())
            pos = children.insert(children.end(), ast);
        else
            *pos = ast;
    }
    else if (pos != children.end())
    {
        children.erase(pos);
    }
}

ASTPtr ASTTTLElement::getExpression(ASTList::iterator pos, bool clone) const
{
    return pos != children.end() ? (clone ? (*pos)->clone() : *pos) : ASTPtr{};
}

}
