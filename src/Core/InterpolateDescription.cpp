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

        std::unordered_map<std::string, size_t> result_index_by_name;
        for (const ColumnWithTypeAndName & column : actions.getResultColumns())
        {
            std::string name = column.name;
            if (const auto & p = aliases.find(name); p != aliases.end())
                name = p->second->getColumnName();

            /// Several outputs may resolve to one destination (aliases collapsing to one column). Each destination
            /// is listed once in `result_columns_order`; `output_to_result_index` maps every output name to it so
            /// the executed interpolate block, which still has one column per output, is routed by name.
            if (result_columns_set.insert(name).second)
            {
                result_index_by_name[name] = result_columns_order.size();
                result_columns_order.push_back(name);
            }
            output_to_result_index[column.name] = result_index_by_name[name];
        }
    }
}
