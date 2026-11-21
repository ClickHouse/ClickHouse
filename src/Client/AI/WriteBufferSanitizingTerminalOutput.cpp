#include <Client/AI/WriteBufferSanitizingTerminalOutput.h>

#include <algorithm>
#include <array>

namespace DB
{

namespace
{

constexpr size_t buffer_size = 4096;

constexpr std::array<char, 3> replacement_character{static_cast<char>(0xEF), static_cast<char>(0xBF), static_cast<char>(0xBD)};

/// The UTF-8 encoding of the Unicode "Control Picture" of a C0 control or DEL: U+2400 + byte for
/// 0x00..0x1F, and U+2421 for DEL. All of them are `E2 90 (0x80 + code)`.
std::array<char, 3> controlPicture(unsigned char byte)
{
    const unsigned char code = byte == 0x7F ? 0x21 : byte;
    return {static_cast<char>(0xE2), static_cast<char>(0x90), static_cast<char>(0x80 + code)};
}

/// The length of the UTF-8 sequence that `byte` starts, or 0 if it cannot start one.
size_t sequenceLength(unsigned char byte)
{
    if (byte < 0x80)
        return 1;
    if (byte < 0xC2)
        return 0;
    if (byte < 0xE0)
        return 2;
    if (byte < 0xF0)
        return 3;
    if (byte < 0xF5)
        return 4;
    return 0;
}

bool isContinuation(unsigned char byte)
{
    return (byte & 0xC0) == 0x80;
}

}

WriteBufferSanitizingTerminalOutput::WriteBufferSanitizingTerminalOutput(WriteBuffer & out_)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size)
    , out(out_)
{
}

WriteBufferSanitizingTerminalOutput::~WriteBufferSanitizingTerminalOutput()
{
    if (!finalized && !canceled)
        cancel();
}

void WriteBufferSanitizingTerminalOutput::process(const char * begin, const char * end)
{
    const char * run_begin = begin;
    const auto flush_run = [&](const char * pos)
    {
        if (pos != run_begin)
            out.write(run_begin, pos - run_begin);
    };

    const char * pos = begin;
    while (pos != end)
    {
        const auto byte = static_cast<unsigned char>(*pos);
        if (byte < 0x80)
        {
            if ((byte < 0x20 && byte != '\n' && byte != '\t') || byte == 0x7F)
            {
                flush_run(pos);
                const auto picture = controlPicture(byte);
                out.write(picture.data(), picture.size());
                run_begin = pos + 1;
            }
            ++pos;
            continue;
        }

        const size_t length = sequenceLength(byte);
        size_t actual = 1;
        while (actual < length && pos + actual != end && isContinuation(static_cast<unsigned char>(pos[actual])))
            ++actual;

        if (length != 0 && actual < length && pos + actual == end)
        {
            /// The chunk ends in the middle of the sequence: it is judged once the rest arrives.
            flush_run(pos);
            std::copy(pos, end, pending);
            pending_size = end - pos;
            return;
        }

        const bool is_c1_control = byte == 0xC2 && actual == 2 && static_cast<unsigned char>(pos[1]) <= 0x9F;
        if (length == 0 || actual < length || is_c1_control)
        {
            /// A C1 control, a byte that cannot start a sequence (a stray continuation byte among
            /// them), or a sequence cut short by a byte that does not continue it.
            flush_run(pos);
            out.write(replacement_character.data(), replacement_character.size());
            pos += is_c1_control ? 2 : actual;
            run_begin = pos;
            continue;
        }

        pos += actual;
    }
    flush_run(end);
}

void WriteBufferSanitizingTerminalOutput::nextImpl()
{
    const char * begin = working_buffer.begin();
    const char * const end = begin + offset();

    if (pending_size)
    {
        /// Complete the held back sequence with the first bytes of this chunk, at most three.
        char joined[4 + 3]{};
        std::copy(pending, pending + pending_size, joined);
        const size_t taken = std::min<size_t>(3, end - begin);
        std::copy(begin, begin + taken, joined + pending_size);
        const size_t joined_size = pending_size + taken;
        const size_t held = pending_size;
        pending_size = 0;

        /// Only as many of the joined bytes as belong to the held back sequence are consumed here.
        const size_t length = sequenceLength(static_cast<unsigned char>(joined[0]));
        size_t actual = 1;
        while (actual < length && actual < joined_size && isContinuation(static_cast<unsigned char>(joined[actual])))
            ++actual;

        if (actual == length)
        {
            process(joined, joined + actual);
        }
        else if (actual == joined_size)
        {
            /// This chunk did not complete the sequence either.
            std::copy(joined, joined + actual, pending);
            pending_size = actual;
        }
        else
        {
            /// A byte that does not continue the sequence cut it short.
            out.write(replacement_character.data(), replacement_character.size());
        }
        begin += actual - held;
    }

    process(begin, end);
    out.next();
}

void WriteBufferSanitizingTerminalOutput::finalizeImpl()
{
    next();
    if (pending_size)
    {
        /// The output ended in the middle of a sequence.
        pending_size = 0;
        out.write(replacement_character.data(), replacement_character.size());
    }
    out.next();
}

}
