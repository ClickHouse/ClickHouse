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
        {
            columns_full_set.insert(name_type.name);
            required_columns_map[name_type.name] = name_type;
        }

        const Block & res_block = actions->getSampleBlock();
        size_t idx = 0;
        for (const ColumnWithTypeAndName & column : res_block)
        {
            columns_full_set.insert(column.name);
            result_columns_map[column.name] = idx++;
        }
    }

}
