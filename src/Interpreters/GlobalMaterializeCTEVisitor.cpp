#include "GlobalMaterializeCTEVisitor.h"
#include <Interpreters/Context.h>
#include "Common/tests/gtest_global_context.h"
#include "Parsers/ASTSelectIntersectExceptQuery.h"
#include "Parsers/ASTSelectWithUnionQuery.h"


namespace DB
{

void DB::GlobalMaterializeCTEVisitor::visit(ASTPtr & ast)
{
    if (auto * explain = ast->as<ASTExplainQuery>())
    {
        for (auto & child : explain->children)
            visit(child);
    }
    else if (auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & child : select_with_union->list_of_selects->children)
            visit(child);
    }
    else if (auto * select_with_intersect_except = ast->as<ASTSelectIntersectExceptQuery>())
    {
        for (auto & child : select_with_intersect_except->getListOfSelects())
            visit(child);
    }
    else if (auto * select = ast->as<ASTSelectQuery>())
    {
        auto cte_list = select->with();
        if (!cte_list)
            return;
        for (auto & child : cte_list->children)
        {
            /// WTH t AS MATERIALIZED (subquery)
            if (auto * with = child->as<ASTWithElement>(); with->has_materialized_keyword)
            {
                data.addExternalStorage(*with, {});
                child = nullptr;
            }
        }
        /// Remove null children
        cte_list->children.erase(std::remove(cte_list->children.begin(), cte_list->children.end(), nullptr), cte_list->children.end());
        if (cte_list->children.empty())
            select->setExpression(ASTSelectQuery::Expression::WITH, nullptr);
    }
}

void GlobalMaterializeCTEVisitor::Data::addExternalStorage(ASTWithElement & cte_expr, const Names & required_columns)
{
    String external_table_name = cte_expr.name;

    auto interpreter = interpretSubquery(cte_expr.subquery, getContext(), subquery_depth, required_columns);

    Block sample = interpreter->getSampleBlock();
    NamesAndTypesList columns = sample.getNamesAndTypesList();

    auto external_storage_holder = std::make_shared<TemporaryTableHolder>(
        getContext(),
        ColumnsDescription{columns},
        ConstraintsDescription{},
        nullptr,
        /*create_for_global_subquery*/ true);
    StoragePtr external_storage = external_storage_holder->getTable();
    getContext()->addExternalTable(external_table_name, std::move(*external_storage_holder));
    FutureTableFromCTE future_table;
    future_table.external_table = external_storage;
    future_table.source = std::make_unique<QueryPlan>();
    interpreter->buildQueryPlan(*future_table.source);
    if (future_tables.emplace(external_table_name, std::move(future_table)).second)
        LOG_DEBUG(getLogger("GlobalMaterializedCTEMatcher"), "Created external table {} for materialized CTE", external_table_name);
    else
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table from CTE with name {} already exists", external_table_name);
}
}
