#include <Interpreters/replaceLegacyToTime.h>

#include <Core/Settings.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTWithAlias.h>

#include <Poco/String.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_legacy_to_time;
}

std::string_view legacyToTimeReplacement(const Settings & settings)
{
    if (settings[Setting::use_legacy_to_time])
        return "toTimeWithFixedDate";

    /// An explicitly requested new meaning is an intention worth persisting too: without it the
    /// stored raw `toTime` would flip to the legacy meaning on a reload under a legacy default
    /// profile. A session at the plain default expressed nothing, so the spelling is kept.
    if (settings[Setting::use_legacy_to_time].changed)
        return "toTimeWithoutDate";

    return {};
}

bool replaceLegacyToTime(IAST & ast, std::string_view replacement)
{
    bool changed = false;

    /// The case-insensitive match mirrors the legacy remapping in `FunctionFactory::tryGetImpl`.
    if (auto * function = ast.as<ASTFunction>(); function && Poco::toLower(function->name) == "totime")
    {
        function->name = String(replacement);
        changed = true;
    }

    const IAST * select_expression_list = nullptr;
    if (const auto * select = ast.as<ASTSelectQuery>())
        select_expression_list = select->select().get();

    for (const auto & child : ast.children)
    {
        if (child.get() != select_expression_list)
        {
            changed |= replaceLegacyToTime(*child, replacement);
            continue;
        }

        for (const auto & expression : child->children)
        {
            const auto * with_alias = dynamic_cast<const ASTWithAlias *>(expression.get());

            String original_name;
            if (with_alias && with_alias->alias.empty())
                original_name = expression->getColumnName();

            if (replaceLegacyToTime(*expression, replacement))
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
            changed |= replaceLegacyToTime(*group_by_key, replacement);
        for (const auto & group_by_assignment : ttl_element->group_by_assignments)
            changed |= replaceLegacyToTime(*group_by_assignment, replacement);
    }

    return changed;
}

}
