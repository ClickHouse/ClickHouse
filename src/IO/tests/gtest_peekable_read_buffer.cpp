#include <gtest/gtest.h>

#include <base/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/PeekableReadBuffer.h>

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

TEST(PeekableReadBuffer, RepeatedEOFWithCheckpointData)
{
    std::string input = "abc";
    DB::ReadBufferFromString source(input);
    DB::PeekableReadBuffer peekable(source);

    ASSERT_FALSE(peekable.eof());
    peekable.setCheckpoint();
    readAndAssert(peekable, "abc");
    ASSERT_TRUE(peekable.eof());
    peekable.rollbackToCheckpoint(true);

    /// After replaying the checkpoint, the final bytes live in peekable's own
    /// memory. An EOF probe must not discard its bookkeeping.
    readAndAssert(peekable, "abc");
    EXPECT_TRUE(peekable.eof());
    EXPECT_TRUE(peekable.eof());
}

namespace
{
class ReadBufferReplacingStorageAtEOF : public DB::ReadBuffer
{
public:
    ReadBufferReplacingStorageAtEOF() : ReadBuffer(nullptr, 0)
    {
        BufferBase::set(first, sizeof(first), 0);
    }

    char * replacementBegin() { return replacement; }

private:
    bool nextImpl() override
    {
        BufferBase::set(replacement, sizeof(replacement), 0);
        return false;
    }

    char first[3] = {'a', 'b', 'c'};
    char replacement[3] = {'d', 'e', 'f'};
};
}

TEST(PeekableReadBuffer, EOFWithReplacedBackingStorage)
{
    ReadBufferReplacingStorageAtEOF source;
    DB::PeekableReadBuffer peekable(source);

    readAndAssert(peekable, "abc");
    ASSERT_TRUE(peekable.eof());

    /// The old bytes are no longer guaranteed to exist after nextImpl().
    EXPECT_EQ(peekable.internalBuffer().begin(), source.replacementBegin());
    EXPECT_TRUE(peekable.eof());
}
