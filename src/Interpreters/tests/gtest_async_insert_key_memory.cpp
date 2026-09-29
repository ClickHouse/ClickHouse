#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Core/Settings.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>
#include <thread>

using namespace DB;

TEST(AsyncInsertKey, RetainedCopyDoesNotChargeProducerOrCleanupQuery)
{
#if defined(SANITIZER)
    GTEST_SKIP() << "Requires ClickHouse allocation interceptors, which sanitizer builds replace";
#else
    const String sql = "INSERT INTO test VALUES (1)";
    ParserInsertQuery parser(sql.data() + sql.size(), false);
    auto ast = parseQuery(parser, sql, DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    Settings settings;
    settings.set("log_comment", String(4 * 1024 * 1024, 'x'));
    AsynchronousInsertQueue::InsertQuery source(ast, {}, {}, {}, {}, {}, {}, {}, {}, settings, AsynchronousInsertQueueDataKind::Parsed);

    std::thread([&]
    {
        ThreadStatus producer;
        producer.untracked_memory_limit = 0;
        MemoryTracker query(&total_memory_tracker, VariableContext::Process);
        std::shared_ptr<const AsynchronousInsertQueue::InsertQuery> retained;
        Int64 charged = 0;
        {
            MemoryTrackerSwitcher scope(&query);
            retained = source.cloneForQueue();
            CurrentThread::flushUntrackedMemory();
            charged = query.get();
        }
        EXPECT_EQ(charged, 0);
        EXPECT_EQ(*retained, source);
        EXPECT_NE(retained->settings.get(), source.settings.get());
        std::weak_ptr<const AsynchronousInsertQueue::InsertQuery> weak = retained;

        /// A background flush or a later query can release the last strong and weak references.
        std::thread([&]
        {
            ThreadStatus consumer;
            consumer.untracked_memory_limit = 0;
            MemoryTracker other_query(&total_memory_tracker, VariableContext::Process);
            Int64 object_delta = 0;
            Int64 control_block_delta = 0;
            {
                MemoryTrackerSwitcher scope(&other_query);
                auto sentinel = std::make_unique<char[]>(16 * 1024 * 1024);
                const auto before = other_query.get();
                retained.reset();
                CurrentThread::flushUntrackedMemory();
                object_delta = other_query.get() - before;
                weak.reset();
                CurrentThread::flushUntrackedMemory();
                control_block_delta = other_query.get() - before - object_delta;
            }
            EXPECT_EQ(object_delta, 0);
            EXPECT_EQ(control_block_delta, 0);
        }).join();
    }).join();
#endif
}
