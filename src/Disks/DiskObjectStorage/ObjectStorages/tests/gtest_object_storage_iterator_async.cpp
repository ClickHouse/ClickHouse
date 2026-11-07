#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <Interpreters/Context.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>

#include <atomic>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageAzureThreads;
    extern const Metric ObjectStorageAzureThreadsActive;
    extern const Metric ObjectStorageAzureThreadsScheduled;
}

namespace DB::ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

using namespace DB;

namespace
{

/// An async list iterator whose pages are fully scripted, so we can reproduce a
/// backend (e.g. Azure Blob Storage) that returns an empty page together with a
/// continuation token ("there are more pages") in the middle of a listing.
class ScriptedIteratorAsync : public IObjectStorageIteratorAsync
{
public:
    struct Page
    {
        std::vector<std::string> names;   /// object keys returned by this page (may be empty)
        bool has_next;                    /// whether the backend reports more pages after this one
    };

    explicit ScriptedIteratorAsync(std::deque<Page> pages_)
        : IObjectStorageIteratorAsync(
            CurrentMetrics::ObjectStorageAzureThreads,
            CurrentMetrics::ObjectStorageAzureThreadsActive,
            CurrentMetrics::ObjectStorageAzureThreadsScheduled,
            ThreadName::AZURE_LIST_POOL)
        , pages(std::move(pages_))
    {
    }

    ~ScriptedIteratorAsync() override
    {
        if (!deactivated)
            deactivate();
    }

    size_t getServedPages() const { return served_pages.load(); }

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        if (pages.empty())
            return false;

        Page page = pages.front();
        pages.pop_front();
        ++served_pages;

        for (const auto & name : page.names)
            batch.emplace_back(std::make_shared<RelativePathWithMetadata>(name));

        return page.has_next;
    }

    std::deque<Page> pages;
    std::atomic<size_t> served_pages{0};
};

using Pages = std::deque<ScriptedIteratorAsync::Page>;

/// Consume element-wise: for (; isValid(); next()) current(). This path is keyed off
/// is_finished, so it also guards that the iterator is not marked finished while a
/// non-empty page is still buffered and unread.
std::vector<std::string> consumeElementWise(Pages pages)
{
    ScriptedIteratorAsync it(std::move(pages));
    std::vector<std::string> result;
    while (it.isValid())
    {
        result.push_back(it.current()->getPath());
        it.next();
    }
    return result;
}

/// Consume batch-wise, as StorageObjectStorageSource and ObjectStorageQueueSource do:
/// via getCurrentBatchAndScheduleNext().
std::vector<std::string> consumeBatchWise(Pages pages)
{
    ScriptedIteratorAsync it(std::move(pages));
    std::vector<std::string> result;
    while (auto batch = it.getCurrentBatchAndScheduleNext())
        for (const auto & object : *batch)
            result.push_back(object->getPath());
    return result;
}

}

/// An empty page that carries a continuation token must not end the listing: the
/// iterator has to keep following the token. Azure Blob Storage returns such
/// empty-page-with-marker responses (e.g. at internal partition boundaries), and
/// treating them as end-of-listing silently skips every object on later pages.
/// Both consumption APIs must return every object, including the last non-empty page.
TEST(ObjectStorageIteratorAsync, EmptyPageWithContinuationTokenDoesNotStopListing)
{
    auto pages = [] { return Pages{
        {{}, true},              /// empty page, but more pages follow
        {{"a", "b"}, true},      /// real objects, still more pages
        {{}, true},              /// empty intermediate page
        {{"c"}, false},          /// last objects, no more pages
    }; };
    const std::vector<std::string> expected{"a", "b", "c"};

    EXPECT_EQ(consumeBatchWise(pages()), expected);
    EXPECT_EQ(consumeElementWise(pages()), expected);
}

/// Sanity check: an empty listing with no continuation token is correctly finished.
TEST(ObjectStorageIteratorAsync, EmptyListingIsFinished)
{
    auto pages = [] { return Pages{{{}, false}}; };
    EXPECT_TRUE(consumeBatchWise(pages()).empty());
    EXPECT_TRUE(consumeElementWise(pages()).empty());
}

/// A stopped query must not keep following a long run of empty pages: nextBatch() follows
/// them on the caller thread, so the caller gets no chance to check for a stop in between.
TEST(ObjectStorageIteratorAsync, CancelledQueryStopsFollowingEmptyPages)
{
    Pages pages(1000, ScriptedIteratorAsync::Page{{}, true});
    pages.push_back({{"a"}, false});

    std::optional<int> error_code;
    size_t served_pages = 0;

    /// A dedicated thread, so the thread group does not depend on what other gtests left behind.
    std::thread t([&]
    {
        ThreadStatus thread_status;
        auto group = std::make_shared<ThreadGroup>(getContext().context, 0);
        group->setQueryCancellationPredicates(
            [] { return true; },
            [] { throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled"); });
        ThreadGroupSwitcher switcher(group, ThreadName::TCP_HANDLER);

        ScriptedIteratorAsync it(std::move(pages));
        try
        {
            it.getCurrentBatchAndScheduleNext();
        }
        catch (const Exception & e)
        {
            error_code = e.code();
        }
        it.deactivate();
        served_pages = it.getServedPages();
    });
    t.join();

    EXPECT_EQ(error_code, ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_LE(served_pages, 3u);
}
