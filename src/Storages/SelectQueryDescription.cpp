#include <Storages/SelectQueryDescription.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLTask.h>

#include <unordered_map>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
extern const int NOT_IMPLEMENTED;
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

/// CTE name -> declarations from the outermost to the innermost visible WITH list.
using VisibleCTEs = std::unordered_map<String, std::vector<const ASTWithElement *>>;

VisibleCTEs withVisibleCTEs(const ASTSelectQuery & query, VisibleCTEs visible)
{
    if (query.with())
        for (const auto & child : query.with()->children)
            if (const auto * with_element = child->as<ASTWithElement>())
                visible[with_element->name].push_back(with_element);
    return visible;
}

const ASTSelectWithUnionQuery & getCTEBody(const ASTWithElement & with_element)
{
    return with_element.subquery->children.at(0)->as<const ASTSelectWithUnionQuery &>();
}

/// The innermost CTE an unqualified first table expression refers to, if any. The returned `visible`
/// no longer contains that declaration: inside its body the name means the enclosing declaration or a table.
const ASTWithElement * followReferencedCTE(const ASTSelectQuery & query, VisibleCTEs & visible)
{
    auto db_and_table = getDatabaseAndTable(query, 0);
    if (!db_and_table || !db_and_table->database.empty())
        return nullptr;

    auto it = visible.find(db_and_table->table);
    if (it == visible.end())
        return nullptr;

    const ASTWithElement * with_element = it->second.back();
    it->second.pop_back();
    if (it->second.empty())
        visible.erase(it);
    return with_element;
}

StorageID extractDependentTableFromSelectQuery(ASTSelectQuery & query, const VisibleCTEs & enclosing)
{
    auto visible = withVisibleCTEs(query, enclosing);

    if (const auto * cte = followReferencedCTE(query, visible))
    {
        auto & cte_query = getCTEBody(*cte).list_of_selects->children.at(0)->as<ASTSelectQuery &>();
        return extractDependentTableFromSelectQuery(cte_query, visible);
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

        auto & inner_query = ast_select->list_of_selects->children.at(0);

        return extractDependentTableFromSelectQuery(inner_query->as<ASTSelectQuery &>(), visible);
    }
    return StorageID::createEmpty();
}


void checkAllowedQueries(const ASTSelectWithUnionQuery & select, const VisibleCTEs & enclosing)
{
    for (const auto & children : select.list_of_selects->children)
    {
        auto * query = children->as<ASTSelectQuery>();

        if (query == nullptr)
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "MATERIALIZED VIEW support query with multiple simple UNION [ALL] only");

        if (query->prewhere() || query->final() || query->sampleSize())
            throw Exception(ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW, "MATERIALIZED VIEW cannot have PREWHERE, SAMPLE or FINAL.");

        auto visible = withVisibleCTEs(*query, enclosing);
        if (const auto * cte = followReferencedCTE(*query, visible))
        {
            checkAllowedQueries(getCTEBody(*cte), visible);
            continue;
        }

        ASTPtr subquery = extractTableExpression(*query, 0);
        if (!subquery)
            return;

        if (const auto * ast_select_with_union = subquery->as<ASTSelectWithUnionQuery>())
        {
            checkAllowedQueries(*ast_select_with_union, visible);
        }
    }
}

}

bool SelectQueryDescription::fixesGlobalWithSetting(const IAST & select)
{
    if (const auto * set_query = select.as<ASTSetQuery>())
    {
        for (const auto & change : set_query->changes)
            if (change.name == "enable_global_with_statement")
                return true;
        for (const auto & reset_name : set_query->default_settings)
            if (reset_name == "enable_global_with_statement")
                return true;
    }
    for (const auto & child : select.children)
        if (child && fixesGlobalWithSetting(*child))
            return true;
    return false;
}

void SelectQueryDescription::checkSettingsAllowedInMatView(const IAST & select, const ContextPtr & context)
{
    auto txn = context->getZooKeeperMetadataTransaction();
    const bool is_initial_query = !txn || txn->isInitialQuery();
    if (is_initial_query && fixesGlobalWithSetting(select))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Setting `enable_global_with_statement` is not supported in a materialized view definition: "
            "the query of a materialized view is always analyzed and executed with it enabled.");
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

    /// Runs first: an arm that is not a `SELECT` is rejected here, before the cast below.
    checkAllowedQueries(query, {});

    /// We trigger only for the first found table
    ASTSelectQuery & new_inner_query = query.list_of_selects->children.at(0)->as<ASTSelectQuery &>();

    /// Qualify the stored first SELECT as before, keeping references to MATERIALIZED CTEs unqualified.
    AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase());
    visitor.setKeptCTEReferences(ApplyWithSubqueryVisitor::visit(query));
    visitor.visit(new_inner_query);

    /// Extracting first found table ID, looking through CTE references
    result.select_table_id = extractDependentTableFromSelectQuery(new_inner_query, {});
    result.inner_query = new_inner_query.clone();

    return result;
}

}
