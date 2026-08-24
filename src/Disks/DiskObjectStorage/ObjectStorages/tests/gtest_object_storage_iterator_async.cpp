#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>

#include <deque>
#include <memory>
#include <string>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageAzureThreads;
    extern const Metric ObjectStorageAzureThreadsActive;
    extern const Metric ObjectStorageAzureThreadsScheduled;
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

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        if (pages.empty())
            return false;

        Page page = pages.front();
        pages.pop_front();

        for (const auto & name : page.names)
            batch.emplace_back(std::make_shared<RelativePathWithMetadata>(name));

        return page.has_next;
    }

    std::deque<Page> pages;
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
