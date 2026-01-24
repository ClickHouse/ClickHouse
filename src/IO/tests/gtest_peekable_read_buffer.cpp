#include <gtest/gtest.h>

#include <base/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/PeekableReadBuffer.h>
#include <Common/Exception.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }
}

static void readAndAssert(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    std::vector<char> tmp(n);
    buf.readStrict(tmp.data(), n);
    ASSERT_EQ(strncmp(tmp.data(), str, n), 0);
}

static void assertAvailable(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    ASSERT_EQ(buf.available(), n);
    ASSERT_EQ(strncmp(buf.position(), str, n), 0);
}

TEST(PeekableReadBuffer, CheckpointsWorkCorrectly)
try
{
    std::string s1 = "0123456789";
    std::string s2 = "qwertyuiop";
    std::string s3 = "asdfghjkl;";
    std::string s4 = "zxcvbnm,./";

    DB::ConcatReadBuffer concat;
    concat.appendBuffer(std::make_unique<DB::ReadBufferFromString>(s1));
    concat.appendBuffer(std::make_unique<DB::ReadBufferFromString>(s2));
    concat.appendBuffer(std::make_unique<DB::ReadBufferFromString>(s3));
    concat.appendBuffer(std::make_unique<DB::ReadBufferFromString>(s4));
    DB::PeekableReadBuffer peekable(concat, 0);

    ASSERT_TRUE(!peekable.eof());
    assertAvailable(peekable, "0123456789");
    {
        DB::PeekableReadBufferCheckpoint checkpoint{peekable};
        readAndAssert(peekable, "01234");
    }

    assertAvailable(peekable, "56789");

    readAndAssert(peekable, "56");

    peekable.setCheckpoint();
    readAndAssert(peekable, "789qwertyu");
    peekable.rollbackToCheckpoint();
    peekable.dropCheckpoint();
    assertAvailable(peekable, "789");

    {
        DB::PeekableReadBufferCheckpoint checkpoint{peekable, true};
        peekable.ignore(20);
    }
    assertAvailable(peekable, "789qwertyuiop");

    readAndAssert(peekable, "789qwertyu");
    peekable.setCheckpoint();
    readAndAssert(peekable, "iopasdfghj");
    assertAvailable(peekable, "kl;");
    peekable.dropCheckpoint();

    peekable.setCheckpoint();
    readAndAssert(peekable, "kl;zxcvbnm,./");
    ASSERT_TRUE(peekable.eof());
    ASSERT_TRUE(peekable.eof());
    ASSERT_TRUE(peekable.eof());
    peekable.rollbackToCheckpoint();
    readAndAssert(peekable, "kl;zxcvbnm");
    peekable.dropCheckpoint();

    ASSERT_TRUE(peekable.hasUnreadData());
    readAndAssert(peekable, ",./");
    ASSERT_FALSE(peekable.hasUnreadData());

    ASSERT_TRUE(peekable.eof());
    ASSERT_TRUE(peekable.eof());
    ASSERT_TRUE(peekable.eof());

}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}

TEST(PeekableReadBuffer, RecursiveCheckpointsWorkCorrectly)
try
{

    std::string s1 = "0123456789";
    std::string s2 = "qwertyuiop";

    DB::ConcatReadBuffer concat;
    concat.appendBuffer(std::make_unique<DB::ReadBufferFromString>(s1));
    concat.appendBuffer(std::make_unique<DB::ReadBufferFromString>(s2));
    DB::PeekableReadBuffer peekable(concat, 0);

    ASSERT_TRUE(!peekable.eof());
    assertAvailable(peekable, "0123456789");
    readAndAssert(peekable, "01234");
    peekable.setCheckpoint();

    readAndAssert(peekable, "56");

    peekable.setCheckpoint();
    readAndAssert(peekable, "78");
    assertAvailable(peekable, "9");
    peekable.rollbackToCheckpoint();
    assertAvailable(peekable, "789");

    readAndAssert(peekable, "789");
    peekable.setCheckpoint();
    readAndAssert(peekable, "qwert");
    peekable.rollbackToCheckpoint();
    assertAvailable(peekable, "qwertyuiop");
    peekable.dropCheckpoint();

    readAndAssert(peekable, "qwerty");
    peekable.setCheckpoint();
    readAndAssert(peekable, "ui");
    peekable.rollbackToCheckpoint();
    assertAvailable(peekable, "uiop");
    peekable.dropCheckpoint();

    peekable.rollbackToCheckpoint();
    assertAvailable(peekable, "789");
    peekable.dropCheckpoint();

    readAndAssert(peekable, "789");
    readAndAssert(peekable, "qwerty");
    peekable.rollbackToCheckpoint();
    assertAvailable(peekable, "56789");
    peekable.dropCheckpoint();

    readAndAssert(peekable, "56789q");
    assertAvailable(peekable, "wertyuiop");
    ASSERT_TRUE(!peekable.hasUnreadData());
    readAndAssert(peekable, "wertyuiop");
    ASSERT_TRUE(peekable.eof());
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}

/// A ReadBuffer that throws an exception after reading some data.
/// Used to test that PeekableReadBuffer::rollbackToCheckpoint() resets the canceled flag.
class ThrowingReadBuffer : public DB::ReadBuffer
{
public:
    ThrowingReadBuffer(const std::string & data_, size_t throw_after_bytes_)
        : ReadBuffer(nullptr, 0), data(data_), throw_after_bytes(throw_after_bytes_), current_pos(0)
    {
    }

private:
    bool nextImpl() override
    {
        if (current_pos >= throw_after_bytes)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Test exception from ThrowingReadBuffer");

        if (current_pos >= data.size())
            return false;

        size_t to_read = std::min(chunk_size, data.size() - current_pos);
        working_buffer = DB::BufferBase::Buffer(const_cast<char *>(data.data()) + current_pos, const_cast<char *>(data.data()) + current_pos + to_read);
        current_pos += to_read;
        return true;
    }

    std::string data;
    size_t throw_after_bytes;
    size_t current_pos;
    static constexpr size_t chunk_size = 5;
};

TEST(PeekableReadBuffer, RollbackResetsTheCanceledFlag)
try
{
    /// Test that rollbackToCheckpoint() resets the canceled flag.
    /// This is important for format auto-detection where we try multiple formats
    /// and some may fail with exceptions.

    std::string data = "0123456789abcdefghij";

    /// Create a buffer that will throw after reading 10 bytes
    ThrowingReadBuffer throwing_buf(data, 10);
    DB::PeekableReadBuffer peekable(throwing_buf, 0);

    peekable.setCheckpoint();

    /// Read some data (less than throw_after_bytes)
    readAndAssert(peekable, "01234");

    /// This should throw because we're past the threshold
    ASSERT_THROW(
    {
        while (!peekable.eof())
            peekable.ignore();
    }, DB::Exception);

    /// After the exception, the buffer should be canceled
    ASSERT_TRUE(peekable.isCanceled());

    /// Rollback should reset the canceled flag
    peekable.rollbackToCheckpoint();
    ASSERT_FALSE(peekable.isCanceled());

    /// Now we should be able to read again from the checkpoint
    /// (though the underlying buffer might still throw if we read too far)
    assertAvailable(peekable, "01234");
    readAndAssert(peekable, "01234");

    peekable.dropCheckpoint();
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}
