#include <Core/Block.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/InterpolateDescription.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

    InterpolateDescription::InterpolateDescription(ExpressionActionsPtr actions_)
        : actions(actions_)
    {
        for (const auto & name_type : actions->getRequiredColumnsWithTypes())
            required_columns_map[name_type.name] = name_type.type;

        for (const ColumnWithTypeAndName & column : actions->getSampleBlock())
            result_columns_map.insert(column.name);
    }

}
