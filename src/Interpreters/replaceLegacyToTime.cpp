#include <Interpreters/replaceLegacyToTime.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTWithAlias.h>

#include <Poco/String.h>

namespace DB
{

bool replaceLegacyToTime(IAST & ast)
{
    bool changed = false;

    /// The case-insensitive match mirrors the legacy remapping in `FunctionFactory::tryGetImpl`.
    if (auto * function = ast.as<ASTFunction>(); function && Poco::toLower(function->name) == "totime")
    {
        function->name = "toTimeWithFixedDate";
        changed = true;
    }

    const IAST * select_expression_list = nullptr;
    if (const auto * select = ast.as<ASTSelectQuery>())
        select_expression_list = select->select().get();

    for (const auto & child : ast.children)
    {
        if (child.get() != select_expression_list)
        {
            changed |= replaceLegacyToTime(*child);
            continue;
        }

        for (const auto & expression : child->children)
        {
            const auto * with_alias = dynamic_cast<const ASTWithAlias *>(expression.get());

            String original_name;
            if (with_alias && with_alias->alias.empty())
                original_name = expression->getColumnName();

            if (replaceLegacyToTime(*expression))
            {
                changed = true;
                if (!original_name.empty())
                    expression->setAlias(original_name);
            }
        }
    }

    /// TTL GROUP BY keys and assignments are not `children` of their `ASTTTLElement`.
    if (auto * ttl_element = ast.as<ASTTTLElement>())
    {
        for (const auto & group_by_key : ttl_element->group_by_key)
            changed |= replaceLegacyToTime(*group_by_key);
        for (const auto & group_by_assignment : ttl_element->group_by_assignments)
            changed |= replaceLegacyToTime(*group_by_assignment);
    }

    return changed;
}

}
