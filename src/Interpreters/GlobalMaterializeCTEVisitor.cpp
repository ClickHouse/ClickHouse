#include <Interpreters/GlobalMaterializeCTEVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/interpretSubquery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/ConstraintsDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
}

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
            /// WITH t AS MATERIALIZED (subquery)
            if (auto * with = child->as<ASTWithElement>(); with && with->has_materialized_keyword)
            {
                data.addExternalStorage(*with);
                child = nullptr;
            }
        }
        /// Remove null child from WITH list
        cte_list->children.erase(std::remove(cte_list->children.begin(), cte_list->children.end(), nullptr), cte_list->children.end());
        if (cte_list->children.empty())
            select->setExpression(ASTSelectQuery::Expression::WITH, nullptr);
    }
}

void GlobalMaterializeCTEVisitor::Data::addExternalStorage(const ASTWithElement & cte_expr)
{
    String external_table_name = cte_expr.name;
    auto & select_query = cte_expr.subquery->as<ASTSubquery &>().children.at(0);
    auto context = getContext();
    std::variant<std::unique_ptr<InterpreterSelectWithUnionQuery>, std::unique_ptr<InterpreterSelectQueryAnalyzer>> interpreter;
    Block sample;
    if (context->getSettingsRef().allow_experimental_analyzer)
    {
        interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(select_query, Context::createCopy(context), SelectQueryOptions().subquery());
        sample = std::get<1>(interpreter)->getSampleBlock();
    }
    else
    {
        interpreter = std::make_unique<InterpreterSelectWithUnionQuery>(select_query, Context::createCopy(context), SelectQueryOptions().subquery());
        sample = std::get<0>(interpreter)->getSampleBlock();
    }

    NamesAndTypesList columns = sample.getNamesAndTypesList();

    TemporaryTableHolder external_storage_holder(
        getContext(),
        ColumnsDescription{columns},
        ConstraintsDescription{},
        /*query*/ nullptr,
        /*delay_read*/ true,
        cte_expr.engine);

    StoragePtr external_storage = external_storage_holder.getTable();
    auto future_table = std::make_shared<FutureTableFromCTE>();

    future_table->name = std::move(external_table_name);
    future_table->external_table = std::move(external_storage);
    future_table->source = std::make_unique<QueryPlan>();
    if (context->getSettingsRef().allow_experimental_analyzer)
        *future_table->source = std::move(*std::get<1>(interpreter)).extractQueryPlan();
    else
        std::get<0>(interpreter)->buildQueryPlan(*future_table->source);

    future_tables.push_back(future_table);
    context->addExternalTableFromCTE(std::move(future_table), std::move(external_storage_holder));
}
}
