#include <Core/Block.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/InterpolateDescription.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

    InterpolateDescription::InterpolateDescription(ExpressionActionsPtr actions_, const Aliases & aliases)
        : actions(actions_)
    {
        for (const auto & name_type : actions->getRequiredColumnsWithTypes())
        {
            if (const auto & p = aliases.find(name_type.name); p != aliases.end())
                required_columns_map[p->second->getColumnName()] = name_type;
            else
                required_columns_map[name_type.name] = name_type;
        }

        for (const ColumnWithTypeAndName & column : actions->getSampleBlock())
        {
            if (const auto & p = aliases.find(column.name); p != aliases.end())
                result_columns_map.insert(p->second->getColumnName());
            else
                result_columns_map.insert(column.name);
        }
    }

}
