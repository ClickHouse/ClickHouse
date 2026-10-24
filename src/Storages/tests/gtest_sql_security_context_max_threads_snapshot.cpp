#include <gtest/gtest.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

ContextMutablePtr makeQueryContext()
{
    auto query_context = Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    return query_context;
}

}

/// The body of a `DEFINER` or `NONE` view runs under a context created from the global context, but it is a part
/// of the outer query, so it must be planned with the outer query's snapshot of free memory and CPU slots.
/// A materialized view is planned while its parent `INSERT` already runs: a fresh snapshot would count the parent.
TEST(SQLSecurityContext, SharesQueryMaxThreadsSnapshot)
{
    auto query_context = makeQueryContext();
    ASSERT_NE(query_context->getQueryMaxThreadsSnapshot(), nullptr);

    StorageInMemoryMetadata metadata;
    ASTSQLSecurity sql_security;
    sql_security.type = SQLSecurityType::NONE;
    metadata.setSQLSecurity(sql_security);

    auto view_context = metadata.getSQLSecurityOverriddenContext(query_context);
    EXPECT_EQ(view_context->getQueryMaxThreadsSnapshot(), query_context->getQueryMaxThreadsSnapshot());

    /// An independent query takes its own snapshot.
    auto other_query_context = makeQueryContext();
    EXPECT_NE(other_query_context->getQueryMaxThreadsSnapshot(), query_context->getQueryMaxThreadsSnapshot());
}
