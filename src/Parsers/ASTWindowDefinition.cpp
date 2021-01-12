#include <Parsers/ASTWindowDefinition.h>

namespace DB
{

ASTPtr ASTWindowDefinition::clone() const
{
    auto result = std::make_shared<ASTWindowDefinition>();

    if (partition_by)
    {
        result->partition_by = partition_by->clone();
        result->children.push_back(result->partition_by);
    }

    if (order_by)
    {
        result->order_by = order_by->clone();
        result->children.push_back(result->order_by);
    }

    return result;
}

String ASTWindowDefinition::getID(char) const
{
    return "WindowDefinition";
}

ASTPtr ASTWindowListElement::clone() const
{
    auto result = std::make_shared<ASTWindowListElement>();

    result->name = name;
    result->definition = definition->clone();
    result->children.push_back(result->definition);

    return result;
}

String ASTWindowListElement::getID(char) const
{
    return "WindowListElement";
}

}
