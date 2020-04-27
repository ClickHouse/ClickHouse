
#include <Columns/Collator.h>
#include <Common/quoteString.h>
#include <Parsers/ASTTTLElement.h>


namespace DB
{

ASTPtr ASTTTLElement::clone() const 
{
    auto clone = std::make_shared<ASTTTLElement>(*this);
    clone->children.clear();
    clone->positions.clear();
    
    for (auto expr : {Expression::TTL, Expression::WHERE})
        clone->setExpression(expr, getExpression(expr, true));

    for (auto & [name, expr] : clone->group_by_aggregations)
        expr = expr->clone();

    return clone;
}

void ASTTTLElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ttl()->formatImpl(settings, state, frame);
    if (mode == Mode::MOVE && destination_type == PartDestinationType::DISK)
    {
        settings.ostr << " TO DISK " << quoteString(destination_name);
    }
    else if (mode == Mode::MOVE && destination_type == PartDestinationType::VOLUME)
    {
        settings.ostr << " TO VOLUME " << quoteString(destination_name);
    }
    else if (mode == Mode::GROUP_BY)
    {
        settings.ostr << " GROUP BY ";
        for (size_t i = 0; i < group_by_key_columns.size(); ++i)
        {
            settings.ostr << group_by_key_columns[i];
            if (i + 1 != group_by_key_columns.size())
                settings.ostr << ", ";
        }
        settings.ostr << " SET ";
        for (size_t i =  0; i < group_by_aggregations.size(); ++i)
        {
            settings.ostr << group_by_aggregations[i].first << " = ";
            group_by_aggregations[i].second->formatImpl(settings, state, frame);
            if (i + 1 != group_by_aggregations.size())
                settings.ostr << ", ";
        }
    }
    else if (mode == Mode::DELETE)
    {
        /// It would be better to output "DELETE" here but that will break compatibility with earlier versions.
    }

    if (where()) 
    {
        settings.ostr << " WHERE ";
        where()->formatImpl(settings, state, frame);
    }
}

void ASTTTLElement::setExpression(Expression expr, ASTPtr && ast) 
{
    auto it = positions.find(expr);
    if (it == positions.end())
    {
        positions[expr] = children.size();
        children.emplace_back(ast);
    }
    else
        children[it->second] = ast;
}

ASTPtr ASTTTLElement::getExpression(Expression expr, bool clone) const
{
    auto it = positions.find(expr);
    if (it != positions.end())
        return clone ? children[it->second]->clone() : children[it->second];
    return {};
}

}
