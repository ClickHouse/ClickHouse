#include "IndexAdvisor.h"
#include <algorithm>
#include <unordered_set>
#include <Core/Settings.h>
#include <Interpreters/IndexAdvisor/TableManager.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

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
extern const SettingsUInt64 max_pk_columns_count;
}

std::pair<Strings, UInt64> IndexAdvisor::getBestPKColumnsForTable(const String & table)
{
    TableManager table_manager(context, workload, table);
    auto columns = table_manager.getColumns();
    const size_t max_columns_in_pk = context->getSettingsRef()[Setting::max_pk_columns_count];

    std::vector<Strings> all_solutions;
    std::vector<Strings> current_best_solutions = {{}};
    UInt64 best_score = UINT64_MAX;

    std::vector<Strings> new_best_solutions;
    UInt64 new_best_score = best_score;
    for (size_t current_len = 1; current_len <= max_columns_in_pk; ++current_len)
    {
        LOG_INFO(getLogger("IndexAdvisor"), "Current length: {}", current_len);
        for (const auto & solution : current_best_solutions)
        {
            LOG_INFO(getLogger("IndexAdvisor"), "Current solution: {}", makePKExpr(solution));
            for (const auto & column : columns)
            {
                if (std::find(solution.begin(), solution.end(), column) != solution.end())
                    continue;

                Strings new_solution = solution;
                new_solution.push_back(column);

                auto pk_expr = makePKExpr(new_solution);
                auto new_estimation = table_manager.estimate(pk_expr);
                LOG_INFO(getLogger("IndexAdvisor"), "New estimation for {} is {}", pk_expr, new_estimation);
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
            all_solutions = new_best_solutions;
        }
        else
        {
            break;
        }

        current_best_solutions = new_best_solutions;
    }

    if (all_solutions.empty())
        return {{}, UINT64_MAX};

    Strings best_solutions;
    for (const auto & solution : all_solutions)
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
