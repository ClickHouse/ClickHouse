#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Interpreters/Context_fwd.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <string_view>

namespace DB
{
class ActionsDAG;
class NamesAndTypesList;
}

namespace DeltaLake
{

class StatsPruner
{
public:
    StatsPruner(
        const DB::ActionsDAG & filter_dag,
        const DB::NamesAndTypesList & table_schema,
        const DB::Names & partition_columns,
        const DB::NameToNameMap & physical_names_map,
        DB::ContextPtr context);

    [[nodiscard]] bool canBePruned(std::string_view stats_json) const;

private:
    std::optional<DB::KeyCondition> key_condition;
    DB::KeyDescription key;
    DB::Names physical_names;
    DB::DataTypes stat_types;
};

}

#endif
