#include <Core/Block.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/InterpolateDescription.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

    InterpolateDescription::InterpolateDescription(ActionsDAGPtr actions_, const Aliases & aliases)
        : actions(actions_)
    {
        for (const auto & name_type : actions->getRequiredColumns())
        {
            if (const auto & p = aliases.find(name_type.name); p != aliases.end())
                required_columns_map[p->second->getColumnName()] = name_type;
            else
                required_columns_map[name_type.name] = name_type;
        }

        for (const ColumnWithTypeAndName & column : actions->getResultColumns())
        {
            std::string name = column.name;
            if (const auto & p = aliases.find(name); p != aliases.end())
                name = p->second->getColumnName();

            result_columns_set.insert(name);
            result_columns_order.push_back(name);
        }
    }

}
