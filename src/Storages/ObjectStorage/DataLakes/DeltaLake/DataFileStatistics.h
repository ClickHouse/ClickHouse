#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/KeyCondition.h>
#include "delta_kernel_ffi.hpp"

#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace DB
{
class ActionsDAG;
}

namespace DeltaLake
{

struct StatisticsColumn
{
    DB::NameAndTypePair column;
    std::string physical_name;
};

using StatisticsColumns = std::vector<StatisticsColumn>;

[[nodiscard]] StatisticsColumns getStatisticsColumns(
    const DB::NamesAndTypesList & table_schema,
    const DB::Names & partition_columns,
    const DB::NameToNameMap & physical_names_map);

struct ColumnStatistics
{
    std::optional<DB::Field> min;
    std::optional<DB::Field> max;
    std::optional<UInt64> null_count;
};

struct DataFileStatistics
{
    std::optional<UInt64> num_records;
    std::vector<ColumnStatistics> columns;

    [[nodiscard]] size_t allocatedBytes() const;
};

class DataFileStatisticsConverter
{
public:
    explicit DataFileStatisticsConverter(const StatisticsColumns & columns_);

    [[nodiscard]] DataFileStatistics convert(const ::Stats & stats) const;

private:
    const StatisticsColumns & columns;
    std::unordered_map<std::string_view, size_t> column_index_by_physical_name;
};

class StatisticsPruner
{
public:
    StatisticsPruner(const DB::ActionsDAG & filter_dag, const StatisticsColumns & columns, DB::ContextPtr context);

    [[nodiscard]] bool canBePruned(const DataFileStatistics & statistics) const;

private:
    struct ColumnCondition
    {
        size_t column_index;
        DB::DataTypes data_types;
        DB::KeyCondition condition;
    };

    std::vector<ColumnCondition> conditions;
};

}

#endif
