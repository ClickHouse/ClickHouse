#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

/** Writes the data to another buffer, replacing the invalid UTF-8 sequences with the specified sequence.
    * If the valid UTF-8 is already written, it works faster.
    * Note: before using the resulting string, destroy this object.
    */
class WriteBufferValidUTF8 final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    static const size_t DEFAULT_SIZE;

    explicit WriteBufferValidUTF8(
        WriteBuffer & output_buffer_,
        bool group_replacements_ = true,
        const char * replacement_ = "\xEF\xBF\xBD",
        size_t size = DEFAULT_SIZE);

    ~WriteBufferValidUTF8() override;

private:
    void putReplacement();
    void putValid(char * data, size_t len);

    void nextImpl() override;
    void finalizeImpl() override;

    WriteBuffer & output_buffer;
    bool group_replacements;
    /// The last recorded character was `replacement`.
    bool just_put_replacement = false;
    std::string replacement;
};

}
