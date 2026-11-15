#include <Storages/SelectQueryDescription.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
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

/// A parenthesized operand of a set operation with a `SETTINGS` clause of its own keeps its
/// `ASTSelectWithUnionQuery` wrapper (see `ParserUnionQueryElement`): `SELECT ... UNION ALL (SELECT ... SETTINGS x = 1)`.
/// It is still a simple `SELECT`, and the clause stays on the `ASTSelectQuery`, so look through the wrapper.
ASTSelectQuery * getSimpleSelectQuery(const ASTPtr & operand)
{
    if (auto * query = operand->as<ASTSelectQuery>())
        return query;
    if (const auto * union_query = operand->as<ASTSelectWithUnionQuery>())
    {
        if (union_query->list_of_selects->children.size() == 1)
            return union_query->list_of_selects->children[0]->as<ASTSelectQuery>();
    }
    return nullptr;
}

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
                subquery->formatForErrorMessage());

        auto * inner_query = getSimpleSelectQuery(ast_select->list_of_selects->children.at(0));
        if (!inner_query)
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "MATERIALIZED VIEW support query with multiple simple UNION [ALL] only");

        return extractDependentTableFromSelectQuery(*inner_query, context, false);
    }
    return StorageID::createEmpty();
}


void checkAllowedQueries(const ASTSelectWithUnionQuery & select)
{
    for (const auto & children : select.list_of_selects->children)
    {
        auto * query = getSimpleSelectQuery(children);

        if (query == nullptr)
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "MATERIALIZED VIEW support query with multiple simple UNION [ALL] only");

        if (query->prewhere() || query->final() || query->sampleSize())
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "MATERIALIZED VIEW cannot have PREWHERE, SAMPLE or FINAL.");

        ASTPtr subquery = extractTableExpression(*query, 0);
        if (!subquery)
            return;

        if (const auto * ast_select_with_union = subquery->as<ASTSelectWithUnionQuery>())
        {
            checkAllowedQueries(*ast_select_with_union);
        }
    }
}

}

SelectQueryDescription SelectQueryDescription::getSelectQueryFromASTForMatView(const ASTPtr & select, bool refreshable, ContextPtr context)
{
    SelectQueryDescription result;
    result.select_query = select->as<ASTSelectWithUnionQuery &>().clone();

    ASTSelectWithUnionQuery & query = result.select_query->as<ASTSelectWithUnionQuery &>();

    /// Skip all the checks, none of them apply to refreshable views.
    /// Don't assign select_table_id. This way no materialized view dependency gets registered,
    /// so data doesn't get pushed to the refreshable view on source table inserts.
    if (refreshable)
        return result;

    checkAllowedQueries(query);
    /// We trigger only for the first found table
    ASTSelectQuery & new_inner_query = *getSimpleSelectQuery(query.list_of_selects->children.at(0));
    /// Extracting first found table ID
    result.select_table_id = extractDependentTableFromSelectQuery(new_inner_query, context);
    result.inner_query = new_inner_query.clone();

    return result;
}

}
