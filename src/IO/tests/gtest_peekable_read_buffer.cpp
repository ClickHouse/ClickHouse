#include <gtest/gtest.h>

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/PeekableReadBuffer.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

static void readAndAssert(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    char tmp[n];
    buf.readStrict(tmp, n);
    ASSERT_EQ(strncmp(tmp, str, n), 0);
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
    DB::ReadBufferFromString b1(s1);
    DB::ReadBufferFromString b2(s2);
    DB::ReadBufferFromString b3(s3);
    DB::ReadBufferFromString b4(s4);

    DB::ConcatReadBuffer concat({&b1, &b2, &b3, &b4});
    DB::PeekableReadBuffer peekable(concat, 0, 16);

    ASSERT_TRUE(!peekable.eof());
    assertAvailable(peekable, "0123456789");
    {
        DB::PeekableReadBufferCheckpoint checkpoint{peekable};
        readAndAssert(peekable, "01234");
    }
    bool exception = false;
    try
    {
        peekable.rollbackToCheckpoint();
    }
    catch (DB::Exception & e)
    {
        if (e.code() != DB::ErrorCodes::LOGICAL_ERROR)
            throw;
        exception = true;
    }
    ASSERT_TRUE(exception);
    assertAvailable(peekable, "56789");

    readAndAssert(peekable, "56");

    peekable.setCheckpoint();
    readAndAssert(peekable, "789qwertyu");
    peekable.rollbackToCheckpoint();
    peekable.dropCheckpoint();
    assertAvailable(peekable, "789");

    exception = false;
    try
    {
        DB::PeekableReadBufferCheckpoint checkpoint{peekable, true};
        peekable.ignore(30);
    }
    catch (DB::Exception & e)
    {
        if (e.code() != DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            throw;
        exception = true;
    }
    ASSERT_TRUE(exception);
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

