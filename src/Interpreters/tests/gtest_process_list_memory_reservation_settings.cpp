#include <gtest/gtest.h>

#include <base/scope_guard.h>
#include <Core/Defines.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/QueryScope.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/tests/gtest_global_context.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{
namespace
{

ASTPtr parseResource(const String & query)
{
    ParserCreateResourceQuery parser;
    return parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
}

ASTPtr parseWorkload(const String & query)
{
    ParserCreateWorkloadQuery parser;
    return parseQuery(parser, query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
}

TEST(ProcessList, MapsMemoryReservationSettingsFromQueryAndServerSettings)
{
    auto global_context = Context::createCopy(getContext().context);

    const auto previous_recovery_reserved
        = global_context->getServerSettings().get("memory_reservation_recovery_reserved_bytes");
    SCOPE_EXIT({
        global_context->setServerSetting(
            "memory_reservation_recovery_reserved_bytes", previous_recovery_reserved);
    });

    global_context->setServerSetting("memory_reservation_recovery_reserved_bytes", UInt64{3333});

    auto storage = global_context->getWorkloadEntityStoragePtr();
    Settings storage_settings;
    auto resource = parseResource("CREATE RESOURCE memory_for_process_list_test (MEMORY RESERVATION)");
    ASSERT_TRUE(storage->storeEntity(
        global_context,
        WorkloadEntityType::Resource,
        "memory_for_process_list_test",
        resource,
        true,
        false,
        storage_settings));
    auto workload = parseWorkload("CREATE WORKLOAD process_list_test SETTINGS max_memory = 1000000");
    ASSERT_TRUE(storage->storeEntity(
        global_context,
        WorkloadEntityType::Workload,
        "process_list_test",
        workload,
        true,
        false,
        storage_settings));

    auto query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    query_context->setSetting("workload", String{"process_list_test"});
    query_context->setSetting("reserve_memory", UInt64{0});
    query_context->setSetting("memory_reservation_protect_from_eviction", true);
    query_context->getClientInfo().current_user = "process_list_test_user";
    query_context->getClientInfo().current_query_id = "process_list_memory_settings";

    {
        ProcessList process_list;
        auto query_scope = QueryScope::create(query_context);
        auto entry = process_list.insert(
            "SELECT 1",
            0,
            nullptr,
            query_context,
            /*watch_start_nanoseconds=*/0,
            /*is_internal=*/false);

        auto * reservation = entry->getQueryStatus()->getMemoryReservation();
        ASSERT_NE(reservation, nullptr);
        EXPECT_TRUE(reservation->isProtectedFromEviction());
        EXPECT_EQ(reservation->getRecoveryReservedBytes(), 3333u);

        entry.reset();
    }
    query_context.reset();
    storage->removeEntity(global_context, WorkloadEntityType::Workload, "process_list_test", true);
    storage->removeEntity(global_context, WorkloadEntityType::Resource, "memory_for_process_list_test", true);
    storage.reset();
}

}
}
