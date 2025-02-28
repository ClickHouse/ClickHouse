#include <Storages/SelectQueryDescription.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}

SelectQueryDescription::SelectQueryDescription(const SelectQueryDescription & other)
    : select_table_id(other.select_table_id)
    , select_query(other.select_query ? other.select_query->clone() : nullptr)
    , inner_query(other.inner_query ? other.inner_query->clone() : nullptr)
{
}

SelectQueryDescription & SelectQueryDescription::SelectQueryDescription::operator=(const SelectQueryDescription & other)
{
    if (&other == this)
        return *this;

    select_table_id = other.select_table_id;
    if (other.select_query)
        select_query = other.select_query->clone();
    else
        select_query.reset();

    if (other.inner_query)
        inner_query = other.inner_query->clone();
    else
        inner_query.reset();
    return *this;
}


namespace
{

StorageID extractDependentTableFromSelectQuery(ASTSelectQuery & query, ContextPtr context, bool add_default_db = true)
{
    if (add_default_db)
    {
        AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase());
        visitor.visit(query);
    }

    if (auto db_and_table = getDatabaseAndTable(query, 0))
    {
        return StorageID(db_and_table->database, db_and_table->table/*, db_and_table->uuid*/);
    }
    if (auto subquery = extractTableExpression(query, 0))
    {
        auto * ast_select = subquery->as<ASTSelectWithUnionQuery>();
        if (!ast_select)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                "StorageMaterializedView cannot be created from table functions ({})",
                serializeAST(*subquery));
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "UNION is not supported for MATERIALIZED VIEW");

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        return extractDependentTableFromSelectQuery(inner_query->as<ASTSelectQuery &>(), context, false);
    }
    return StorageID::createEmpty();
}


void checkAllowedQueries(const ASTSelectQuery & query)
{
    if (query.prewhere() || query.final() || query.sampleSize())
        throw Exception(DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "MATERIALIZED VIEW cannot have PREWHERE, SAMPLE or FINAL.");

    ASTPtr subquery = extractTableExpression(query, 0);
    if (!subquery)
        return;

    if (const auto * ast_select = subquery->as<ASTSelectWithUnionQuery>())
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "UNION is not supported for MATERIALIZED VIEW");

        const auto & inner_query = ast_select->list_of_selects->children.at(0);

        checkAllowedQueries(inner_query->as<ASTSelectQuery &>());
    }
}

}

/// check if only one single select query in SelectWithUnionQuery
static bool isSingleSelect(const ASTPtr & select, ASTPtr & res)
{
    auto * new_select = select->as<ASTSelectWithUnionQuery>();
    if (new_select == nullptr)
        return false;

    if (new_select->list_of_selects->children.size() != 1)
        return false;

    auto & new_inner_query = new_select->list_of_selects->children.at(0);
    if (new_inner_query->as<ASTSelectQuery>())
    {
        res = new_inner_query;
        return true;
    }

    return isSingleSelect(new_inner_query, res);
}

SelectQueryDescription SelectQueryDescription::getSelectQueryFromASTForMatView(const ASTPtr & select, bool refreshable, ContextPtr context)
{
    SelectQueryDescription result;
    result.select_query = select->as<ASTSelectWithUnionQuery &>().clone();

    /// Skip all the checks, none of them apply to refreshable views.
    /// Don't assign select_table_id. This way no materialized view dependency gets registered,
    /// so data doesn't get pushed to the refreshable view on source table inserts.
    if (refreshable)
        return result;

    ASTPtr new_inner_query;

    if (!isSingleSelect(select, new_inner_query))
        throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "UNION is not supported for MATERIALIZED VIEW");

    auto & select_query = new_inner_query->as<ASTSelectQuery &>();
    checkAllowedQueries(select_query);

    result.select_table_id = extractDependentTableFromSelectQuery(select_query, context);
    result.inner_query = new_inner_query->clone();

    return result;
}

}
