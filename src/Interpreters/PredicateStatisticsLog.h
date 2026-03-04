#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/// PredicateStatisticsLog records per-predicate selectivity statistics
/// collected during query execution in `FilterTransform`
struct PredicateStatisticsLogElement
{
    UInt16 event_date{};
    time_t event_time{};

    String database;
    String table;
    String column_name;
    String predicate_class;       /// "Equality", "Range", "In", "LikeSubstring", "IsNull", "Other"
    String function_name;         /// "equals", "less", ...

    UInt64 input_rows{};
    UInt64 passed_rows{};
    Float64 selectivity{};        /// passed_rows / input_rows (precomputed for convenience)

    String query_id;

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
