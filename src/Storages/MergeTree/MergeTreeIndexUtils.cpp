#include <Storages/MergeTree/MergeTreeIndexUtils.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

ASTPtr buildFilterNode(const ASTPtr & select_query, ASTs additional_filters)
{
    auto & select_query_typed = select_query->as<ASTSelectQuery &>();

    ASTs filters;
    if (select_query_typed.where())
        filters.push_back(select_query_typed.where());

    if (select_query_typed.prewhere())
        filters.push_back(select_query_typed.prewhere());

    filters.insert(filters.end(), additional_filters.begin(), additional_filters.end());

    if (filters.empty())
        return nullptr;

    ASTPtr filter_node;

    if (filters.size() == 1)
    {
        filter_node = filters.front();
    }
    else
    {
        auto function = std::make_shared<ASTFunction>();

        function->name = "and";
        function->arguments = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->arguments);
        function->arguments->children = std::move(filters);

        filter_node = std::move(function);
    }

    return filter_node;
}

}
