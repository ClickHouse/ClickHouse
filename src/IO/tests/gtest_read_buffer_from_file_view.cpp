#include <gtest/gtest.h>

#include <IO/ReadBufferFromFileView.h>

#include <cstring>

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

/// An archive buffer that reads `chunk` bytes per `nextImpl` and ignores the read bound, like a local file,
/// so its buffer may extend past the end of the file in the view. With `discard_buffer_on_bound_change`
/// a new bound discards the buffer and the reading continues from the current position, like `ReadBufferFromS3`.
class ArchiveBuffer : public ReadBufferFromFileBase
{
public:
    ArchiveBuffer(String data_, size_t chunk, bool discard_buffer_on_bound_change_)
        : ReadBufferFromFileBase(chunk, nullptr, 0)
        , data(std::move(data_))
        , discard_buffer_on_bound_change(discard_buffer_on_bound_change_)
    {
    }

    String getFileName() const override { return "archive"; }
    std::optional<size_t> tryGetFileSize() override { return data.size(); }
    off_t getPosition() override { return file_offset - available(); }

    off_t seek(off_t off, int) override
    {
        const size_t new_pos = off;
        if (new_pos <= file_offset && new_pos + working_buffer.size() >= file_offset)
        {
            pos = working_buffer.end() - (file_offset - new_pos);
            return off;
        }

        resetWorkingBuffer();
        file_offset = new_pos;
        return off;
    }

    void setReadUntilPosition(size_t) override
    {
        if (!discard_buffer_on_bound_change)
            return;

        file_offset = getPosition();
        resetWorkingBuffer();
    }

private:
    bool nextImpl() override
    {
        if (file_offset >= data.size())
            return false;

        const size_t size = std::min(internal_buffer.size(), data.size() - file_offset);
        memcpy(internal_buffer.begin(), data.data() + file_offset, size);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + size);
        file_offset += size;
        return true;
    }

    const String data;
    const bool discard_buffer_on_bound_change;
    size_t file_offset = 0;
};

String makeArchive(size_t size)
{
    String data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>(i % 251);
    return data;
}

String readToEnd(ReadBuffer & in)
{
    String result;
    while (!in.eof())
    {
        result.append(in.position(), in.available());
        in.position() = in.buffer().end();
    }
    return result;
}

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
}

TEST(ReadBufferFromFileView, EmptyRequestMapIsNothing)
{
    ByteRangeSet map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);

    view.setRequestMap(makeSet({{0, 10}}));
    view.setRequestMap({});
    expectRanges(map, {});
}

TEST(ReadBufferFromFileView, RequestMapOutsideTheSliceIsNothing)
{
    ByteRangeSet map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);

    view.setRequestMap(makeSet({{150, 5}}));
    expectRanges(map, {});
}

TEST(ReadBufferFromFileView, RequestMapIsShiftedIntoTheSliceAndClipped)
{
    ByteRangeSet map;
    ReadBufferFromFileView view(std::make_unique<MapRecordingBuffer>(map), "file", 100, 200);

    view.setRequestMap(makeSet({{0, 10}, {90, 20}, {150, 5}}));
    expectRanges(map, {{100, 10}, {190, 10}});
}

/// The file occupies [100, 300) of the archive, and each read of the archive buffer goes past its end.
class ReadBufferFromFileViewBound : public ::testing::TestWithParam<bool>
{
protected:
    const String archive = makeArchive(1000);
    const String file = archive.substr(100, 200);
    ReadBufferFromFileView view{std::make_unique<ArchiveBuffer>(archive, 512, GetParam()), "file", 100, 300};

    void readUntil(size_t position)
    {
        view.setReadUntilPosition(position);
        EXPECT_EQ(readToEnd(view), file.substr(0, position));
        EXPECT_EQ(view.getPosition(), static_cast<off_t>(position));
    }
};

TEST_P(ReadBufferFromFileViewBound, RaiseBound)
{
    readUntil(50);

    view.setReadUntilPosition(150);
    EXPECT_EQ(readToEnd(view), file.substr(50, 100));
    EXPECT_EQ(view.getPosition(), 150);

    view.setReadUntilEnd();
    EXPECT_EQ(readToEnd(view), file.substr(150));
    EXPECT_EQ(view.getPosition(), 200);
}

TEST_P(ReadBufferFromFileViewBound, PrefetchKeepsBound)
{
    readUntil(50);

    view.prefetch(Priority{});
    EXPECT_TRUE(view.eof());
    EXPECT_EQ(view.getPosition(), 50);

    view.setReadUntilEnd();
    EXPECT_EQ(readToEnd(view), file.substr(50));
}

TEST_P(ReadBufferFromFileViewBound, RequestMapKeepsBound)
{
    readUntil(50);

    view.setRequestMap(makeSet({{0, 200}}));
    EXPECT_TRUE(view.eof());
    EXPECT_EQ(view.getPosition(), 50);

    view.setReadUntilEnd();
    EXPECT_EQ(readToEnd(view), file.substr(50));
}

TEST_P(ReadBufferFromFileViewBound, SeekKeepsBound)
{
    readUntil(50);

    EXPECT_EQ(view.seek(20, SEEK_SET), 20);
    EXPECT_EQ(readToEnd(view), file.substr(20, 30));

    view.setReadUntilEnd();
    EXPECT_EQ(view.seek(-10, SEEK_CUR), 40);
    EXPECT_EQ(readToEnd(view), file.substr(40));
}

INSTANTIATE_TEST_SUITE_P(DiscardBufferOnBoundChange, ReadBufferFromFileViewBound, ::testing::Bool());
