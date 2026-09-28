#include <gtest/gtest.h>

#include <IO/ReadBufferFromFileView.h>

using namespace DB;

namespace
{

/// An empty archive buffer that keeps the last request map the view passes to it.
class MapRecordingBuffer : public ReadBufferFromFileBase
{
public:
    explicit MapRecordingBuffer(ByteRangeSet & map_)
        : ReadBufferFromFileBase(0, nullptr, 0)
        , map(map_)
    {
    }

    String getFileName() const override { return "archive"; }
    void setRequestMap(ByteRangeSet ranges) override { map = std::move(ranges); }
    off_t seek(off_t off, int) override { return position = off; }
    off_t getPosition() override { return position; }

private:
    bool nextImpl() override { return false; }

    ByteRangeSet & map;
    off_t position = 0;
};

ByteRangeSet makeSet(std::initializer_list<ByteRange> ranges)
{
    ByteRangeSet set;
    for (const auto & range : ranges)
        set.add(range);
    return set;
}

void expectRanges(const ByteRangeSet & map, std::initializer_list<ByteRange> expected)
{
    const auto & ranges = map.ranges();
    ASSERT_EQ(ranges.size(), expected.size());
    size_t i = 0;
    for (const auto & range : expected)
    {
        EXPECT_EQ(ranges[i].offset, range.offset) << "range " << i;
        EXPECT_EQ(ranges[i].size, range.size) << "range " << i;
        ++i;
    }
}

}

TEST(ReadBufferFromFileView, RequestMapIsTheSliceByDefault)
{
    ByteRangeSet map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);
    expectRanges(map, {{100, 100}});

    view.setRequestMap(makeSet({{0, 10}}));
    view.setRequestMap({});
    expectRanges(map, {{100, 100}});
}

TEST(ReadBufferFromFileView, RequestMapIsShiftedIntoTheSliceAndClipped)
{
    ByteRangeSet map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);

    view.setRequestMap(makeSet({{0, 10}, {90, 20}, {150, 5}}));
    expectRanges(map, {{100, 10}, {190, 10}});
}
