#include "ReadFromHiveStep.h"

#if USE_HIVE

namespace DB
{

void ReadFromHiveStep::updateEstimate(MutableColumns & columns) const
{
    if (!analysis_result)
        return;

    size_t index = 0;
    columns[index++]->insert(storage_id.database_name);
    columns[index++]->insert(storage_id.table_name);
    columns[index++]->insert(analysis_result->toObject());
}

}
#endif
