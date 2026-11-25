#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <memory>
#include <optional>
#include <string_view>
#include <unordered_set>
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
    const std::unordered_set<String> & binary_columns,
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

class DataFileStatisticsParser
{
public:
    explicit DataFileStatisticsParser(const StatisticsColumns & columns_);
    ~DataFileStatisticsParser();

    [[nodiscard]] DataFileStatistics parse(std::string_view json);

private:
    struct Impl;

    const StatisticsColumns & columns;
    std::unique_ptr<Impl> impl;
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
