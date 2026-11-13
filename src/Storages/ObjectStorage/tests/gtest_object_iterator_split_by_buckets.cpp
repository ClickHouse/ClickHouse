#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <chrono>
#include <future>
#include <mutex>
#include <set>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto test_format = "SplitByBucketsTestFormat";
constexpr size_t buckets_per_file = 3;

class TestFileBucketInfo : public FileBucketInfo
{
public:
    explicit TestFileBucketInfo(size_t bucket_) : bucket(bucket_) {}
    void serialize(WriteBuffer &, size_t) const override {}
    void deserialize(ReadBuffer &, size_t) override {}
    String getIdentifier() const override { return "_bucket_" + std::to_string(bucket); }
    String getFormatName() const override { return test_format; }
    std::shared_ptr<FileBucketInfo> filterByMatchingRowGroups(const RowGroupIndices &) const override { return nullptr; }

private:
    size_t bucket;
};

class TestSplitter : public IBucketSplitter
{
public:
    VectorWithMemoryTracking<FileBucketInfoPtr> splitToBuckets(
        size_t, ReadBuffer &, const RelativePathWithMetadata &, const FormatSettings &, const ContextPtr &) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The file must be split by byte ranges");
    }

    VectorWithMemoryTracking<FileBucketInfoPtr> splitForParallelReading(
        size_t, ReadBuffer &, const FormatSettings &, const std::optional<RelativePathWithMetadata> &, const ContextPtr &) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected call");
    }

    VectorWithMemoryTracking<FileBucketInfoPtr> splitByByteRanges(size_t, size_t, const VectorWithMemoryTracking<UInt64> &) override
    {
        VectorWithMemoryTracking<FileBucketInfoPtr> result;
        for (size_t bucket = 0; bucket < buckets_per_file; ++bucket)
            result.push_back(std::make_shared<TestFileBucketInfo>(bucket));
        return result;
    }
};

class ObjectInfoWithSize : public ObjectInfo
{
public:
    explicit ObjectInfoWithSize(const String & path) : ObjectInfo(RelativePathWithMetadata(path)) {}
    std::shared_ptr<ObjectInfo> clone() const override { return std::make_shared<ObjectInfoWithSize>(*this); }
    std::optional<size_t> getFileSizeHint() const override { return 100; }
};

class IteratorHoldingLastFile : public IObjectIterator
{
public:
    IteratorHoldingLastFile(std::vector<String> paths_, std::shared_future<void> released_)
        : paths(std::move(paths_)), released(std::move(released_))
    {
    }

    ObjectInfoPtr next(size_t) override
    {
        size_t index = 0;
        {
            std::lock_guard lock(mutex);
            if (next_index == paths.size())
            {
                if (!end_reached)
                {
                    end_reached = true;
                    end_reached_promise.set_value();
                }
                return nullptr;
            }
            index = next_index++;
        }

        if (index + 1 == paths.size())
        {
            last_taken_promise.set_value();
            released.get();
        }
        return std::make_shared<ObjectInfoWithSize>(paths[index]);
    }

    size_t estimatedKeysCount() override { return paths.size(); }

    std::future<void> lastTaken() { return last_taken_promise.get_future(); }
    std::future<void> endReached() { return end_reached_promise.get_future(); }

private:
    const std::vector<String> paths;
    const std::shared_future<void> released;
    std::promise<void> last_taken_promise;
    std::promise<void> end_reached_promise;
    std::mutex mutex;
    size_t next_index = 0;
    bool end_reached = false;
};

ContextMutablePtr makeContext()
{
    static std::once_flag registered;
    std::call_once(registered, [] { FormatFactory::instance().registerSplitter(test_format, [] { return std::make_shared<TestSplitter>(); }); });

    auto context = Context::createCopy(getContext().context);
    context->setSetting("cluster_table_function_split_by_split_offsets", Field(true));
    return context;
}

std::shared_ptr<ObjectIteratorSplitByBuckets> makeSplitIterator(ObjectIterator iterator, const ContextPtr & context)
{
    auto configuration = std::make_shared<StorageLocalConfiguration>();
    configuration->format = test_format;

    return std::make_shared<ObjectIteratorSplitByBuckets>(std::move(iterator), configuration, nullptr, context);
}

std::future<ObjectInfoPtr> nextInBackground(std::shared_ptr<ObjectIteratorSplitByBuckets> iterator)
{
    std::packaged_task<ObjectInfoPtr()> task([iterator] { return iterator->next(0); });
    auto result = task.get_future();
    std::thread(std::move(task)).detach();
    return result;
}

}

TEST(ObjectIteratorSplitByBuckets, ReturnsBucketsOfFileBeingSplitAfterEndOfIterator)
{
    auto context = makeContext();
    std::promise<void> release;
    auto inner = std::make_shared<IteratorHoldingLastFile>(std::vector<String>{"file0", "file1"}, release.get_future().share());
    auto last_taken = inner->lastTaken();
    auto end_reached = inner->endReached();
    auto iterator = makeSplitIterator(inner, context);

    std::set<String> identifiers;
    for (size_t i = 0; i < buckets_per_file; ++i)
        identifiers.insert(iterator->next(0)->getIdentifier());

    auto splitting = nextInBackground(iterator);
    const auto last_taken_status = last_taken.wait_for(std::chrono::seconds(30));

    auto waiting = nextInBackground(iterator);
    const auto end_reached_status = end_reached.wait_for(std::chrono::seconds(30));
    const auto waiting_status = waiting.wait_for(std::chrono::milliseconds(100));

    release.set_value();

    ASSERT_EQ(last_taken_status, std::future_status::ready);
    ASSERT_EQ(end_reached_status, std::future_status::ready);
    ASSERT_EQ(waiting_status, std::future_status::timeout);

    for (auto * future : {&splitting, &waiting})
    {
        auto object = future->get();
        ASSERT_NE(object, nullptr);
        identifiers.insert(object->getIdentifier());
    }
    while (auto object = iterator->next(0))
        identifiers.insert(object->getIdentifier());

    std::set<String> expected;
    for (const auto * path : {"file0", "file1"})
        for (size_t bucket = 0; bucket < buckets_per_file; ++bucket)
            expected.insert(String(path) + "_bucket_" + std::to_string(bucket));
    ASSERT_EQ(identifiers, expected);
    ASSERT_EQ(iterator->next(0), nullptr);
}

TEST(ObjectIteratorSplitByBuckets, WakesWaitingCallerWhenSplittingThrows)
{
    auto context = makeContext();
    std::promise<void> release;
    auto inner = std::make_shared<IteratorHoldingLastFile>(std::vector<String>{"file0"}, release.get_future().share());
    auto last_taken = inner->lastTaken();
    auto end_reached = inner->endReached();
    auto iterator = makeSplitIterator(inner, context);

    auto splitting = nextInBackground(iterator);
    const auto last_taken_status = last_taken.wait_for(std::chrono::seconds(30));

    auto waiting = nextInBackground(iterator);
    const auto end_reached_status = end_reached.wait_for(std::chrono::seconds(30));
    const auto waiting_status = waiting.wait_for(std::chrono::milliseconds(100));

    release.set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR, "Splitting failed")));

    ASSERT_EQ(last_taken_status, std::future_status::ready);
    ASSERT_EQ(end_reached_status, std::future_status::ready);
    ASSERT_EQ(waiting_status, std::future_status::timeout);

    ASSERT_THROW((void)splitting.get(), Exception);
    ASSERT_EQ(waiting.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    ASSERT_EQ(waiting.get(), nullptr);
}
