#include "IndexAdvisor.h"
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_pk_columns_count;
}

Strings IndexAdvisor::getBestPKColumnsForTable(const String & table)
{
    auto columns = table_manager.getColumns(table);
    Strings current_best;
    // UInt64 current_best_estimation; // TODO: use a better value
    bool found_better = true;
    while (found_better && current_best.size() < context->getSettingsRef()[Setting::max_pk_columns_count]) {
        found_better = false;
        Strings best_columns;
        UInt64 best_estimation = UINT64_MAX;
        for (const auto & column : columns)
        {
            if (std::find(current_best.begin(), current_best.end(), column) != current_best.end())
                continue;
            Strings new_best = current_best;
            new_best.push_back(column);
            std::unordered_map<String, Strings> pk_columns = {{table, new_best}};
            auto estimation = table_manager.estimate(pk_columns);
            if (estimation < best_estimation)
            {
                best_estimation = estimation;
                best_columns = new_best;
                found_better = true;
            }
        }
        if (found_better)
        {
            current_best = best_columns;
            // current_best_estimation = best_estimation;
            String columns_str;
            for (const auto & col : current_best)
            {
                columns_str += col + ", ";
            }
            // LOG_INFO(getLogger("IndexAdvisor"), "Found better PK columns for table {}: {} with estimation {}", table, columns_str, current_best_estimation);
        }
    }

    return current_best;
}

std::unordered_map<String, Strings> IndexAdvisor::getBestPKColumns()
{
    std::unordered_map<String, Strings> pk_columns;
    auto tables = table_manager.getTables();
    for (const auto & table : tables)
    {
        if (table.empty())
            continue;
        pk_columns[table] = getBestPKColumnsForTable(table);
        if (pk_columns[table].empty())
        {
            LOG_INFO(getLogger("IndexAdvisor"), "No suitable PK columns found for table: {}", table);
            continue;
        }
    }

    return pk_columns;

}
}
