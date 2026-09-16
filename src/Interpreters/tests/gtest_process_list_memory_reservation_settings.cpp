#include <gtest/gtest.h>

#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/MapConfiguration.h>
#include <fmt/format.h>

#include <cstdint>
#include <filesystem>

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
    SharedContextHolder shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());

    const auto workload_path = std::filesystem::temp_directory_path()
        / fmt::format("clickhouse-process-list-memory-settings-{}", reinterpret_cast<uintptr_t>(shared_context.get()));
    std::filesystem::create_directories(workload_path);

    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration;
    config->setString("workload_path", workload_path.string());
    config->setString("memory_reservation_max_allocation_before_suction_bytes", "1111");
    config->setString("memory_reservation_suction_max_allocation_bytes", "2222");
    config->setString("memory_reservation_suction_reserved_bytes", "3333");
    config->setString("memory_reservation_suction_queue_policy", "largest_memory_first");

    global_context->setConfig(config);
    global_context->setPath(workload_path.string());
    global_context->setApplicationType(Context::ApplicationType::SERVER);

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
    query_context->setSetting("workload", "process_list_test");
    query_context->setSetting("reserve_memory", UInt64{0});
    query_context->setSetting("memory_reservation_protect_from_eviction", true);
    query_context->setSetting("memory_reservation_force_spill_before_eviction", true);
    query_context->setSetting("memory_reservation_suction_queue_timeout_ms", UInt64{444});
    query_context->getClientInfo().current_user = "process_list_test_user";
    query_context->getClientInfo().current_query_id = "process_list_memory_settings";

    ProcessList process_list;
    auto entry = process_list.insert(
        "SELECT 1",
        0,
        nullptr,
        query_context,
        /*watch_start_nanoseconds=*/0,
        /*is_internal=*/false);

    auto * reservation = entry->getQueryStatus()->getMemoryReservation();
    ASSERT_NE(reservation, nullptr);
    const auto & settings = reservation->getSettings();

    EXPECT_TRUE(settings.pressure_policy.protect_from_eviction);
    EXPECT_TRUE(settings.force_spill_before_eviction);
    EXPECT_EQ(settings.suction_queue_timeout_ms, 444u);
    EXPECT_EQ(settings.pressure_policy.max_allocation_before_suction_bytes, 1111u);
    EXPECT_EQ(settings.pressure_policy.suction_max_allocation_bytes, 2222u);
    EXPECT_EQ(settings.pressure_policy.suction_reserved_bytes, 3333u);
    EXPECT_EQ(
        settings.pressure_policy.suction_queue_policy,
        ResourceAllocation::SuctionQueuePolicy::LargestMemoryFirst);

    entry.reset();
    storage->removeEntity(global_context, WorkloadEntityType::Workload, "process_list_test", true);
    storage->removeEntity(global_context, WorkloadEntityType::Resource, "memory_for_process_list_test", true);
    storage.reset();
    query_context.reset();
    global_context.reset();
    shared_context.reset();
    std::filesystem::remove_all(workload_path);
}

}
}
