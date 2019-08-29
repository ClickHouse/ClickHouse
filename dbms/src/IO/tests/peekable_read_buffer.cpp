#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/PeekableReadBuffer.h>


void readAndAssert(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    char tmp[n];
    buf.readStrict(tmp, n);
    assert(strncmp(tmp, str, n) == 0);
}

void assertAvailable(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    assert(buf.available() == n);
    assert(strncmp(buf.position(), str, n) == 0);
}

int main(int, char **)
{
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

        assert(!peekable.eof());
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
        assert(exception);
        assertAvailable(peekable, "56789");

        readAndAssert(peekable, "56");

        peekable.setCheckpoint();
        readAndAssert(peekable, "789qwertyu");
        peekable.rollbackToCheckpoint();
        peekable.dropCheckpoint();
        assertAvailable(peekable, "789");
        peekable.peekNext();
        assertAvailable(peekable, "789qwertyuiop");
        assert(peekable.lastPeeked().size() == 10);
        assert(strncmp(peekable.lastPeeked().begin(), "asdfghjkl;", 10) == 0);

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
        assert(exception);
        assertAvailable(peekable, "789qwertyuiop");
        assert(peekable.lastPeeked().size() == 10);
        assert(strncmp(peekable.lastPeeked().begin(), "asdfghjkl;", 10) == 0);

        readAndAssert(peekable, "789qwertyu");
        peekable.setCheckpoint();
        readAndAssert(peekable, "iopasdfghj");
        assertAvailable(peekable, "kl;");
        peekable.dropCheckpoint();

        peekable.setCheckpoint();
        readAndAssert(peekable, "kl;zxcvbnm,./");
        assert(peekable.eof());
        assert(peekable.eof());
        assert(peekable.eof());
        peekable.rollbackToCheckpoint();
        readAndAssert(peekable, "kl;zxcvbnm");
        peekable.dropCheckpoint();

        exception = false;
        try
        {
            peekable.assertCanBeDestructed();
        }
        catch (DB::Exception & e)
        {
            if (e.code() != DB::ErrorCodes::LOGICAL_ERROR)
                throw;
            exception = true;
        }
        assert(exception);

        auto buf_ptr = peekable.takeUnreadData();
        assert(peekable.eof());
        assert(peekable.eof());
        assert(peekable.eof());

        readAndAssert(*buf_ptr, ",./");
        assert(buf_ptr->eof());

        peekable.assertCanBeDestructed();
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
