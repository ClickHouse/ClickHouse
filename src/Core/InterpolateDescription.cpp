#include <Core/Block.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/InterpolateDescription.h>
#include <Interpreters/convertFieldToType.h>
#include <Core/SettingsEnums.h>
#include <Common/IntervalKind.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{
    InterpolateDescription::InterpolateDescription(ActionsDAG actions_, const Aliases & aliases)
        : actions(std::move(actions_))
    {
        for (const auto & name_type : actions.getRequiredColumns())
        {
            if (const auto & p = aliases.find(name_type.name); p != aliases.end())
                required_columns_map[p->second->getColumnName()] = name_type;
            else
                required_columns_map[name_type.name] = name_type;
        }

        for (const ColumnWithTypeAndName & column : actions.getResultColumns())
        {
            std::string name = column.name;
            if (const auto & p = aliases.find(name); p != aliases.end())
                name = p->second->getColumnName();

            /// Several result columns may resolve to the same block name (e.g. `INTERPOLATE (x AS a, x AS a)`
            /// or aliases collapsing to one column). `result_columns_order` must list each destination once:
            /// FillingTransform maps every entry to a header position, and a repeated position makes it append
            /// to the same output column twice per generated row, producing a ragged (non-rectangular) block.
            if (result_columns_set.insert(name).second)
                result_columns_order.push_back(name);
        }
    }
}
