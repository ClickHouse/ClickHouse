#include <Access/RowPolicy.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <boost/range/algorithm/equal.hpp>

#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_PREWHERE;
}

namespace
{
    /// arrayJoin changes the number of rows. It can hide behind an alias (the case-insensitive
    /// unnest, caught by resolving to the canonical name) or a SQL UDF that is inlined into the
    /// filter at read time (caught by descending into the UDF body). A call inside a nested
    /// subquery has its own scope and does not multiply the outer rows, so it is skipped.
    bool expressionContainsArrayJoin(const ASTPtr & ast, std::unordered_set<String> & visited_udfs)
    {
        if (!ast)
            return false;
        if (const auto * function = ast->as<ASTFunction>())
        {
            if (getFunctionCanonicalNameIfAny(function->name) == "arrayJoin")
                return true;
            if (auto udf_body = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
                udf_body && visited_udfs.insert(function->name).second
                    && expressionContainsArrayJoin(udf_body, visited_udfs))
                return true;
        }
        for (const auto & child : ast->children)
        {
            if (!child->as<ASTSelectQuery>() && expressionContainsArrayJoin(child, visited_udfs))
                return true;
        }
        return false;
    }
}

void checkRowPolicyFilterExpression(const ASTPtr & expression)
{
    std::unordered_set<String> visited_udfs;
    if (expressionContainsArrayJoin(expression, visited_udfs))
        throw Exception(ErrorCodes::ILLEGAL_PREWHERE, "arrayJoin is not allowed in a row policy filter expression");
}


void RowPolicy::setDatabase(const String & database)
{
    full_name.database = database;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setTableName(const String & table_name)
{
    full_name.table_name = table_name;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setShortName(const String & short_name)
{
    full_name.short_name = short_name;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setFullName(const String & short_name, const String & database, const String & table_name)
{
    full_name.short_name = short_name;
    full_name.database = database;
    full_name.table_name = table_name;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setFullName(const RowPolicyName & full_name_)
{
    full_name = full_name_;
    IAccessEntity::setName(full_name.toString());
}

void RowPolicy::setName(const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RowPolicy::setName is not implemented");
}


bool RowPolicy::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_policy = typeid_cast<const RowPolicy &>(other);
    return (full_name == other_policy.full_name) && boost::range::equal(filters, other_policy.filters)
        && restrictive == other_policy.restrictive && (to_roles == other_policy.to_roles);
}

std::vector<UUID> RowPolicy::findDependencies() const
{
    return to_roles.findDependencies();
}

bool RowPolicy::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    return to_roles.hasDependencies(ids);
}

void RowPolicy::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    to_roles.replaceDependencies(old_to_new_ids);
}

void RowPolicy::copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids)
{
    if (getType() != src.getType())
        return;
    const auto & src_policy = typeid_cast<const RowPolicy &>(src);
    to_roles.copyDependenciesFrom(src_policy.to_roles, ids);
}

void RowPolicy::removeDependencies(const std::unordered_set<UUID> & ids)
{
    to_roles.removeDependencies(ids);
}

void RowPolicy::clearAllExceptDependencies()
{
    for (auto & filter : filters)
        filter = {};
}

}
