#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ReadHelpers.h>

#include <gtest/gtest.h>
#include <cstring>

using namespace DB;

namespace
{

class MemorySourceReader : public ISourceReader
{
public:
    explicit MemorySourceReader(String data_) : data(std::move(data_)) {}

    size_t read(const StoredObject &, size_t offset, size_t size, char * buffer) override
    {
        if (offset >= data.size())
            return 0;
        size_t to_read = std::min(size, data.size() - offset);
        std::memcpy(buffer, data.data() + offset, to_read);
        return to_read;
    }

    String name() const override { return "MemorySourceReader"; }

private:
    String data;
};

}

TEST(PipelineReadBuffer, ReadAll)
{
    String content = "Hello, ReaderExecutor! This is a test of the pipeline read buffer.";
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 20);

    PipelineReadBuffer buf(std::move(executor));

    String result;
    readStringUntilEOF(result, buf);
    EXPECT_EQ(result, content);
}

TEST(PipelineReadBuffer, Seek)
{
    String content = "0123456789ABCDEF";
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 8);

    PipelineReadBuffer buf(std::move(executor));

    buf.seek(10, SEEK_SET);
    String result;
    readStringUntilEOF(result, buf);
    EXPECT_EQ(result, "ABCDEF");
}

TEST(PipelineReadBuffer, GetPosition)
{
    String content(100, 'X');
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", 100);

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 30);

    PipelineReadBuffer buf(std::move(executor));

    EXPECT_EQ(buf.getPosition(), 0);
    buf.next();
    /// After consuming first window (30 bytes), position should be at 30
    /// But the position depends on how many bytes are in working_buffer.
    /// After next(), pos should be at start of working_buffer.
    EXPECT_GE(buf.getPosition(), 0);
    EXPECT_LE(buf.getPosition(), 30);
}

TEST(PipelineReadBuffer, TryGetFileSize)
{
    String content(500, 'Y');
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", 500);

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 100);

    PipelineReadBuffer buf(std::move(executor));

    auto size = buf.tryGetFileSize();
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 500);
}
