#include <gtest/gtest.h>

#include <IO/ReadBufferFromFileView.h>

using namespace DB;

namespace
{

/// An empty archive buffer that keeps the last request map the view passes to it.
class MapRecordingBuffer : public ReadBufferFromFileBase
{
public:
    explicit MapRecordingBuffer(VectorWithMemoryTracking<ByteRange> & map_)
        : ReadBufferFromFileBase(0, nullptr, 0)
        , map(map_)
    {
    }

    String getFileName() const override { return "archive"; }
    void setRequestMap(VectorWithMemoryTracking<ByteRange> ranges) override { map = std::move(ranges); }
    off_t seek(off_t off, int) override { return position = off; }
    off_t getPosition() override { return position; }

private:
    bool nextImpl() override { return false; }

    VectorWithMemoryTracking<ByteRange> & map;
    off_t position = 0;
};

void expectRanges(const VectorWithMemoryTracking<ByteRange> & map, std::initializer_list<ByteRange> expected)
{
    ASSERT_EQ(map.size(), expected.size());
    size_t i = 0;
    for (const auto & range : expected)
    {
        EXPECT_EQ(map[i].offset, range.offset) << "range " << i;
        EXPECT_EQ(map[i].size, range.size) << "range " << i;
        ++i;
    }
}

}

TEST(ReadBufferFromFileView, RequestMapIsTheSliceByDefault)
{
    VectorWithMemoryTracking<ByteRange> map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);
    expectRanges(map, {{100, 100}});

    view.setRequestMap({{0, 10}});
    view.setRequestMap({});
    expectRanges(map, {{100, 100}});
}

TEST(ReadBufferFromFileView, RequestMapIsShiftedIntoTheSliceAndClipped)
{
    VectorWithMemoryTracking<ByteRange> map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);

    view.setRequestMap({{0, 10}, {90, 20}, {150, 5}});
    expectRanges(map, {{100, 10}, {190, 10}});
}
