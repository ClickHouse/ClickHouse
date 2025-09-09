#pragma once
#include "config.h"
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ObjectInfoWithPartitionColumns.h>

#if USE_DELTA_KERNEL_RS

namespace DB
{
class ActionsDAG;
class NamesAndTypesList;
using Names = std::vector<std::string>;
}

namespace DeltaLake
{

class PartitionPruner
{
public:
    PartitionPruner(
        const DB::ActionsDAG & filter_dag,
        const DB::NamesAndTypesList & table_schema_,
        const DB::Names & partition_columns_,
        DB::ContextPtr context);

    bool canBePruned(const DB::ObjectInfoWithPartitionColumns & object_info) const;

private:
    std::optional<DB::KeyCondition> key_condition;
    DB::KeyDescription partition_key;
};

}

#endif
