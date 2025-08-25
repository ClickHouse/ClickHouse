#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <IO/Operators.h>

namespace DB
{

void Group::addExpression(GroupExpressionPtr group_expression)
{
    group_expression->group_id = group_id;
    expressions.push_back(std::move(group_expression));
}

void Group::dump(WriteBuffer & out, String indent) const
{
    for (const auto & expression : expressions)
    {
        out << indent << "'" << expression->getDescription() << "' inputs:";
        for (const auto & input_group_id : expression->inputs)
            out << " #" << input_group_id;
        out << "\n";
    }
}


}
