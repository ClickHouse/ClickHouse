#pragma once

#include <IO/ReadHelpers.h>

namespace DB
{

class IReadBufferPrepareAndEndUp
{
public:
    virtual ~IReadBufferPrepareAndEndUp() {}

    virtual void prepareReadBuffer(ReadBuffer & buffer) = 0;

    virtual void endUpReadBuffer(ReadBuffer & buffer) = 0;
};

using IReadBufferPrepareAndEndUpPtr = std::shared_ptr<IReadBufferPrepareAndEndUp>;

class JSONEachRowPrepareAndEndUp : public IReadBufferPrepareAndEndUp
{
public:
    void prepareReadBuffer(ReadBuffer & buffer) override
    {
        /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
        skipBOMIfExists(in);

        skipWhitespaceIfAny(in);
        if (!in.eof() && *in.position() == '[')
        {
            ++in.position();
            data_in_square_brackets = true;
        }
    }

    void endUpReadBuffer(ReadBuffer & buffer) override
    {
        skipWhitespaceIfAny(buffer);
        if (data_in_square_brackets)
        {
            assertChar(']', buffer);
            skipWhitespaceIfAny(buffer);
        }
        if (!buffer.eof() && *buffer.position() == ';')
        {
            ++buffer.position();
            skipWhitespaceIfAny(buffer);
        }
        assertEOF(buffer);
    }

private:
    bool data_in_square_brackets{false};
};

}
