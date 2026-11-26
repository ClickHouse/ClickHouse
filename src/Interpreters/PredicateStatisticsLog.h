#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/// Predicate selectivity from MergeTree prewhere chain and FilterTransform
struct PredicateStatisticsLogElement
{
    UInt16 event_date{};
    time_t event_time{};

    String database;
    String table;
    String query_id;

    /// whole filter expression handled by this step
    String predicate_expression;

    /// this step's selectivity
    UInt64 input_rows{};
    UInt64 passed_rows{};
    Float64 filter_selectivity{}; /// passed_rows / input_rows

    /// whole predicate selectivity (across all steps)
    UInt64 total_input_rows{};
    UInt64 total_passed_rows{};
    Float64 total_selectivity{};

    /// index-level granule stats
    std::vector<String> index_names;
    std::vector<String> index_types;
    std::vector<UInt64> total_granules;
    std::vector<UInt64> granules_after;
    std::vector<Float64> index_selectivities;

    static std::string name() { return "PredicateStatisticsLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class PredicateStatisticsLog : public SystemLog<PredicateStatisticsLogElement>
{
    using SystemLog<PredicateStatisticsLogElement>::SystemLog;
};

}
