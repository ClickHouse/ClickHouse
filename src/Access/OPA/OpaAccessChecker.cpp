#include <Access/OPA/OpaAccessChecker.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/OPA/OpaExpressions.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/quoteString.h>
#include <Parsers/makeASTForLogicalFunction.h>


namespace ProfileEvents
{
    extern const Event OpaDenials;
    extern const Event OpaCacheHits;
    extern const Event OpaCacheMisses;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

OpaAccessChecker::OpaAccessChecker(OpaConfigurationPtr configuration_)
    : configuration(std::move(configuration_))
    , client(configuration)
{
}

bool OpaAccessChecker::governs(const String & user_name, const AccessRightsElement & element) const
{
    /// An administrative or technical account has to keep working while a policy is broken, which is
    /// the only way to repair the configuration that broke it.
    if (configuration->isUserExempt(user_name))
        return false;

    /// A check that names no database is a global privilege - `SYSTEM SHUTDOWN`, introspection,
    /// access management. Those are not objects a data policy describes, and they stay with native
    /// grants; a policy that wanted to restrict them could not name them anyway.
    if (element.anyDatabase())
        return false;

    /// A global privilege with a parameter, such as a named collection or a table engine, carries the
    /// parameter in the database slot, so it would otherwise look like a database-scoped check.
    if (element.isGlobalWithParameter())
        return false;

    return configuration->isDatabaseInScope(element.database);
}

bool OpaAccessChecker::isAllowed(
    const AccessRightsElement & element,
    const OpaRequestContext & request_context,
    const OpaDecisionCachePtr & cache) const
{
    OpaRequest request;

    for (const auto & keyword : element.access_flags.toKeywords())
        request.operations.emplace_back(keyword);

    if (element.anyTable())
    {
        request.resource = OpaResource::forDatabase(element.database);
    }
    else
    {
        /// `anyColumn` means the check is about the table as a whole, so no column list is sent and a
        /// policy can tell that case apart from a check that names columns.
        Names columns;
        if (!element.anyColumn())
            columns = element.columns;

        request.resource = OpaResource::forTable(element.database, element.table, std::move(columns));
    }

    /// The identity is not part of the key: a cache belongs to one query, and a query has one
    /// requesting user throughout.
    const OpaDecisionCache::Key key{request.operations, *request.resource};

    if (cache)
    {
        if (const auto cached = cache->get(key))
        {
            ProfileEvents::increment(ProfileEvents::OpaCacheHits);
            return *cached;
        }
        ProfileEvents::increment(ProfileEvents::OpaCacheMisses);
    }

    const bool decision = client.isAllowed(request, request_context);

    if (cache)
        cache->set(key, decision);

    if (!decision)
        ProfileEvents::increment(ProfileEvents::OpaDenials);

    return decision;
}

RowPolicyFilterPtr OpaAccessChecker::getRowFilter(
    const String & database,
    const String & table,
    const OpaRequestContext & request_context,
    const OpaDecisionCachePtr & cache) const
{
    if (!configuration->hasRowFilters() || !configuration->isDatabaseInScope(database)
        || configuration->isUserExempt(request_context.user))
        return nullptr;

    OpaRequest request;
    request.operations = {"SELECT"};
    request.resource = OpaResource::forTable(database, table);

    const OpaDecisionCache::Key key{request.operations, *request.resource};

    Strings expressions;
    if (cache)
    {
        if (auto cached = cache->getRowFilters(key))
        {
            ProfileEvents::increment(ProfileEvents::OpaCacheHits);
            expressions = std::move(*cached);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::OpaCacheMisses);
            expressions = client.getRowFilters(request, request_context);
            cache->setRowFilters(key, expressions);
        }
    }
    else
    {
        expressions = client.getRowFilters(request, request_context);
    }

    if (expressions.empty())
        return nullptr;

    ASTs parsed;
    parsed.reserve(expressions.size());
    for (const auto & expression : expressions)
        parsed.push_back(parseOpaRowFilterExpression(expression, "row filter"));

    auto filter = std::make_shared<RowPolicyFilter>();
    /// Several filters all apply, so they are combined with AND: a policy can add restrictions but
    /// cannot use a second filter to widen what the first one allowed.
    filter->expression = parsed.size() == 1 ? parsed.front() : makeASTForLogicalAnd(std::move(parsed));
    filter->database_and_table_name = std::make_shared<const std::pair<String, String>>(database, table);

    return filter;
}

std::vector<bool> OpaAccessChecker::filterColumns(
    const Names & operations,
    const String & database,
    const String & table,
    const Names & columns,
    const OpaRequestContext & request_context) const
{
    std::vector<bool> allowed;
    allowed.reserve(columns.size());

    /// Chunked so that a very wide table does not become one enormous request. The chunks are
    /// independent questions, and concatenating their answers is the same as asking about all the
    /// columns at once.
    for (size_t offset = 0; offset < columns.size(); offset += configuration->max_batch_size)
    {
        const size_t count = std::min(configuration->max_batch_size, columns.size() - offset);

        OpaRequest request;
        request.operations = operations;
        request.filter_resources.reserve(count);
        for (size_t i = 0; i < count; ++i)
            request.filter_resources.push_back(OpaResource::forTable(database, table, {columns[offset + i]}));

        const auto chunk = client.filterAllowed(request, request_context);
        allowed.insert(allowed.end(), chunk.begin(), chunk.end());
    }

    return allowed;
}

std::unordered_map<String, ASTPtr> OpaAccessChecker::getColumnMasks(
    const String & database,
    const String & table,
    const Names & columns,
    const OpaRequestContext & request_context,
    const OpaDecisionCachePtr & cache) const
{
    if (!configuration->hasColumnMasking() || !configuration->isDatabaseInScope(database)
        || configuration->isUserExempt(request_context.user) || columns.empty())
        return {};

    OpaRequest request;
    request.operations = {"SELECT"};
    request.resource = OpaResource::forTable(database, table, columns);

    const OpaDecisionCache::Key key{request.operations, *request.resource};

    std::vector<OpaColumnMask> masks;
    if (cache)
    {
        if (auto cached = cache->getColumnMasks(key))
        {
            ProfileEvents::increment(ProfileEvents::OpaCacheHits);
            masks = std::move(*cached);
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::OpaCacheMisses);
            masks = client.getColumnMasks(request, request_context);
            cache->setColumnMasks(key, masks);
        }
    }
    else
    {
        masks = client.getColumnMasks(request, request_context);
    }

    std::unordered_map<String, ASTPtr> result;
    for (const auto & mask : masks)
    {
        auto parsed = parseOpaExpression(mask.expression, "mask for column " + backQuote(mask.column));

        /// Two masks for the same column would make the effective one depend on the order a policy
        /// happened to emit them.
        if (!result.emplace(mask.column, std::move(parsed)).second)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The Open Policy Agent policy returned more than one mask for column {} of table {}.{}",
                backQuote(mask.column),
                backQuoteIfNeed(database),
                backQuoteIfNeed(table));
        }
    }

    return result;
}

}
