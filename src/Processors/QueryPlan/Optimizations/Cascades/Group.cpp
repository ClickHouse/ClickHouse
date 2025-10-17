#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <IO/Operators.h>

namespace DB
{

void Group::addLogicalExpression(GroupExpressionPtr group_expression)
{
    group_expression->group_id = group_id;
    logical_expressions.push_back(std::move(group_expression));
}

void Group::addPhysicalExpression(GroupExpressionPtr group_expression)
{
    group_expression->group_id = group_id;
    physical_expressions.push_back(std::move(group_expression));
}

void Group::dump(WriteBuffer & out, String indent) const
{
    out << indent << "Logical:\n";
    for (const auto & expression : logical_expressions)
    {
        out << indent;
        expression->dump(out);
        out << "\n";
    }

    out << indent << "Physical:\n";
    for (const auto & expression : physical_expressions)
    {
        out << indent;
        expression->dump(out);
        out << "\n";
    }

    if (best_implementation.expression)
    {
        out << indent << "Best:\n"
            << indent << indent
            // << "Rows: " << best_implementation.cost.number_of_rows
            << " Cost: " << best_implementation.cost.subtree_cost << "\n"
            << indent << indent;
        best_implementation.expression->dump(out);
        out << "\n";
    }
}

String Group::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
