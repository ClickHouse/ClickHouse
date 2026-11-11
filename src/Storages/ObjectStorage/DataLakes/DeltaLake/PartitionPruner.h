#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/KeyDescription.h>

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
        const DB::NameToNameMap & physical_names_map_,
        DB::ContextPtr context);

    bool canBePruned(const DB::ObjectInfo & object_info) const;

private:
    std::optional<DB::KeyCondition> key_condition;
    DB::KeyDescription partition_key;
    DB::Names physical_partition_columns;
};

}

#endif
