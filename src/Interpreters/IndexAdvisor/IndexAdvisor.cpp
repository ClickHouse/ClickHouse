#include "IndexAdvisor.h"
#include <algorithm>
#include <Core/Settings.h>
#include <Interpreters/IndexAdvisor/TableManager.h>
#include <Interpreters/IndexAdvisor/IndexManager.h>

namespace DB
{

namespace
{
String makePKExpr(const Strings & columns)
{
    if (columns.empty())
        return "";

    String pk_expr = "(" + columns[0];
    for (size_t i = 1; i < columns.size(); ++i)
        pk_expr += ", " + columns[i];
    pk_expr += ")";
    return pk_expr;
}
}

namespace Setting
{
extern const SettingsUInt64 max_index_advisor_pk_columns_count;
extern const SettingsUInt64 max_index_advise_index_columns_count;
}


IndexTypes IndexAdvisor::getBestMinMaxIndexForTable(const String & table)
{
    IndexManager index_manager(context, workload, table);
    auto columns = index_manager.getColumns();
    std::map<UInt64, std::vector<std::pair<String, String>>> estimations;

    // Estimate indexes for each column with each index type
    for (const auto & index_type : index_types)
    {
        for (const auto & column : columns)
        {
            if (!index_manager.addIndex("test_index_" + column, column, index_type))
                continue;
            auto estimation = index_manager.estimate();
            estimations[estimation].push_back(std::make_pair(column, index_type));
            index_manager.dropIndex("test_index_" + column);
        }
    }

    if (estimations.empty())
        return {};

    // Sort estimations by estimation value (ascending) and return top max_index_advise_index_columns_count
    auto max_index_advise_index_columns_count = context->getSettingsRef()[Setting::max_index_advise_index_columns_count];
    std::vector<std::pair<String, String>> result;
    for (const auto & [estimation, indexed_columns] : estimations) {
        for (const auto & column : indexed_columns) {
            if (result.size() >= max_index_advise_index_columns_count)
                break;
            result.push_back(column);
}
        if (result.size() >= max_index_advise_index_columns_count)
            break;
    }
    return result;
}

std::unordered_map<String, IndexTypes> IndexAdvisor::getBestMinMaxIndexForTables()
{
    std::unordered_map<String, IndexTypes> result;
    for (const auto & table : workload.getTables()) {
        result[table] = getBestMinMaxIndexForTable(table);
    }
    return result;
}

std::pair<Strings, UInt64> IndexAdvisor::getBestPKColumnsForTable(const String & table)
{
    TableManager table_manager(context, workload, table);
    auto columns = table_manager.getColumns();
    const size_t max_columns_in_pk = context->getSettingsRef()[Setting::max_index_advisor_pk_columns_count];

    std::vector<Strings> current_best_solutions = {{}};
    UInt64 best_score = UINT64_MAX;

    // Start with empty solution and try to add columns one by one
    std::vector<Strings> new_best_solutions;
    UInt64 new_best_score = best_score;
    for (size_t current_len = 1; current_len <= max_columns_in_pk; ++current_len)
    {
        for (const auto & solution : current_best_solutions)
        {
            for (const auto & column : columns)
            {
                if (std::find(solution.begin(), solution.end(), column) != solution.end())
                    continue;

                Strings new_solution = solution;
                new_solution.push_back(column);

                auto new_estimation = table_manager.estimate(makePKExpr(new_solution));
                if (new_estimation < new_best_score)
                {
                    new_best_score = new_estimation;
                    new_best_solutions.clear();
                    new_best_solutions.push_back(new_solution);
                }
                else if (new_estimation == new_best_score)
                {
                    new_best_solutions.push_back(new_solution);
                }
            }
        }

        if (new_best_solutions.empty())
        {
            break;
        }

        if (new_best_score < best_score)
        {
            best_score = new_best_score;
            current_best_solutions = new_best_solutions;
        }
        else
        {
            break;
        }
    }

    if (current_best_solutions.empty())
        return {{}, UINT64_MAX};

    Strings best_solutions;
    for (const auto & solution : current_best_solutions)
    {
        String solution_str = makePKExpr(solution);
        best_solutions.push_back(std::move(solution_str));
    }
    return {std::move(best_solutions), best_score};
}

std::unordered_map<String, std::pair<Strings, UInt64>> IndexAdvisor::getBestPKColumns()
{
    std::unordered_map<String, std::pair<Strings, UInt64>> pk_columns;
    for (const auto & table : workload.getTables())
    {
        if (table.empty())
            continue;
        pk_columns[table] = getBestPKColumnsForTable(table);
    }

    return pk_columns;
}
}
