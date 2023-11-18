#include <Interpreters/DDLIdentifiersCollector.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTCreateQuery.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTAlterQuery.h>

namespace DB
{



void DDLQueryIdentifiers::addQueryIdentifiers(const DDLQueryIdentifiers & other)
{
    used_identifiers.insert(other.used_identifiers.begin(), other.used_identifiers.end());
    produced_identifiers.insert(other.produced_identifiers.begin(), other.produced_identifiers.end());
}

void DDLQueryIdentifiers::removeQueryIdentifiers(const DDLQueryIdentifiers & other)
{
    for (const auto & used_identifier : other.used_identifiers)
    {
        auto it = used_identifiers.find(used_identifier);
        if (it != used_identifiers.end())
            used_identifiers.erase(it);
    }

    for (const auto & produced_identifier : other.produced_identifiers)
    {
        auto it = produced_identifiers.find(produced_identifier);
        if (it != produced_identifiers.end())
            produced_identifiers.erase(it);
    }
}

bool hasIntersection(const DDLQueryIdentifiers & left, const DDLQueryIdentifiers & right)
{
    std::vector<std::string> result;
    auto used_it = std::set_intersection(left.used_identifiers.begin(), left.used_identifiers.begin(), right.produced_identifiers.begin(), right.produced_identifiers.end(), result.begin());
    if (used_it != result.begin())
        return true;

    result.clear();

    auto produced_it = std::set_intersection(left.produced_identifiers.begin(), left.produced_identifiers.begin(), right.used_identifiers.begin(), right.used_identifiers.end(), result.begin());
    if (produced_it != result.begin())
        return true;

    return false;
}

DDLQueryIdentifiers collectIdentifiersForDDLQuery(const ASTPtr & ddl_query, ContextPtr context)
{
    DDLQueryIdentifiers result;

    if (auto * rename_query = ddl_query->as<ASTRenameQuery>(); rename_query != nullptr)
    {
        for (const auto & table_element : rename_query->elements)
        {
            result.used_identifiers.insert(table_element.from.getTable());
            result.produced_identifiers.insert(table_element.to.getTable());
        }
        return result;
    }

    ASTQueryWithTableAndOutput * query_with_output = dynamic_cast<ASTQueryWithTableAndOutput *>(ddl_query.get());

    if (query_with_output == nullptr)
    {
        auto identifiers = IdentifiersCollector::collect(ddl_query);

        for (const auto & identifier : identifiers)
            result.used_identifiers.insert(identifier->name());

        return result;
    }

    std::string main_produced_identifier = query_with_output->getTable();
    result.produced_identifiers.insert(main_produced_identifier);

    auto identifiers = AllIdentifiersCollector::collect(ddl_query);

    for (const auto & identifier : identifiers)
        result.used_identifiers.insert(identifier->name());

    result.used_identifiers.erase(main_produced_identifier);

    StorageID storage_id(*query_with_output);

    if (ddl_query->as<ASTCreateQuery>() || ddl_query->as<ASTAlterQuery>())
    {
        auto create_dependencies = getDependenciesFromCreateQuery(context, storage_id.getQualifiedName(), ddl_query);
        for (const auto & depedency : create_dependencies)
            result.used_identifiers.insert(depedency.getFullName());
    }

    return result;
}


}
