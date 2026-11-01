#include <gtest/gtest.h>

#include <Poco/BufferedStreamBuf.h>
#include <ostream>
#include <sstream>
#include <vector>


/// Mock BufferedStreamBuf that simulates short writes from the device.
/// Each call to writeToDevice writes at most `max_write_size` bytes.
class ShortWriteStreamBuf : public Poco::BufferedStreamBuf
{
public:
    ShortWriteStreamBuf(int buffer_size, int max_write_size_)
        : Poco::BufferedStreamBuf(buffer_size, std::ios::out)
        , max_write_size(max_write_size_)
    {
    }

    const std::string & written() const { return output; }
    int writeCount() const { return write_calls; }

private:
    int writeToDevice(const char * buffer, std::streamsize length) override
    {
        ++write_calls;
        int to_write = std::min(static_cast<int>(length), max_write_size);
        output.append(buffer, to_write);
        return to_write;
    }

    std::string output;
    int max_write_size;
    int write_calls = 0;
};


/// Mock that fails after writing a certain number of bytes total.
class FailAfterNStreamBuf : public Poco::BufferedStreamBuf
{
public:
    FailAfterNStreamBuf(int buffer_size, int fail_after_)
        : Poco::BufferedStreamBuf(buffer_size, std::ios::out)
        , fail_after(fail_after_)
    {
    }

    const std::string & written() const { return output; }

private:
    int writeToDevice(const char * buffer, std::streamsize length) override
    {
        if (total_written >= fail_after)
            return 0;

        int to_write = std::min(static_cast<int>(length), fail_after - total_written);
        output.append(buffer, to_write);
        total_written += to_write;
        return to_write;
    }

    std::string output;
    int fail_after;
    int total_written = 0;
};


TEST(BufferedStreamBuf, FlushHandlesShortWrites)
{
    /// Buffer size 16, device writes at most 3 bytes per call.
    ShortWriteStreamBuf buf(16, 3);
    std::ostream os(&buf);

    std::string data = "Hello, World!"; /// 13 bytes
    os.write(data.data(), data.size());
    os.flush();

    ASSERT_TRUE(os.good()) << "Stream should be in good state after flush";
    ASSERT_EQ(buf.written(), data);
    /// 13 bytes with max 3 per call = at least 5 writeToDevice calls
    ASSERT_GE(buf.writeCount(), 5);
}


TEST(BufferedStreamBuf, FlushHandlesShortWritesSingleByte)
{
    /// Worst case: device writes 1 byte at a time.
    ShortWriteStreamBuf buf(32, 1);
    std::ostream os(&buf);

    std::string data = "Short write test with single byte device";
    os.write(data.data(), data.size());
    os.flush();

    ASSERT_TRUE(os.good());
    ASSERT_EQ(buf.written(), data);
    ASSERT_EQ(buf.writeCount(), static_cast<int>(data.size()));
}


TEST(BufferedStreamBuf, FlushHandlesFullWriteInOneCall)
{
    /// Device writes everything in one call -- loop should iterate once.
    ShortWriteStreamBuf buf(64, 1000);
    std::ostream os(&buf);

    std::string data = "All at once";
    os.write(data.data(), data.size());
    os.flush();

    ASSERT_TRUE(os.good());
    ASSERT_EQ(buf.written(), data);
    ASSERT_EQ(buf.writeCount(), 1);
}


TEST(BufferedStreamBuf, FlushSetsErrorOnWriteFailure)
{
    /// Device fails immediately (returns 0).
    FailAfterNStreamBuf buf(16, 0);
    std::ostream os(&buf);
    os.exceptions(std::ios::badbit);

    std::string data = "Will fail";
    os.write(data.data(), data.size());

    ASSERT_THROW(os.flush(), std::ios_base::failure);
}


TEST(BufferedStreamBuf, FlushWritesPartialThenFails)
{
    /// Device writes 5 bytes then fails.
    FailAfterNStreamBuf buf(32, 5);
    std::ostream os(&buf);
    os.exceptions(std::ios::badbit);

    std::string data = "1234567890"; /// 10 bytes, device fails after 5
    os.write(data.data(), data.size());

    ASSERT_THROW(os.flush(), std::ios_base::failure);
    /// The first 5 bytes should have been written before failure.
    ASSERT_EQ(buf.written(), "12345");
}


TEST(BufferedStreamBuf, OverflowTriggersShortWriteLoop)
{
    /// Buffer size 8, device writes 3 bytes at a time.
    /// Writing 20 bytes should trigger overflow (flushing the 8-byte buffer
    /// via the short-write loop) then continue filling the buffer.
    ShortWriteStreamBuf buf(8, 3);
    std::ostream os(&buf);

    std::string data = "01234567ABCDEFGHIJKL"; /// 20 bytes
    os.write(data.data(), data.size());
    os.flush();

    ASSERT_TRUE(os.good());
    ASSERT_EQ(buf.written(), data);
}
