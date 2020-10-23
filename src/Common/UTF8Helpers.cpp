#include <Common/UTF8Helpers.h>

#include <widechar_width.h>

namespace DB
{
namespace UTF8
{

// based on https://bjoern.hoehrmann.de/utf-8/decoder/dfa/
// Copyright (c) 2008-2009 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions: The above copyright
// notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

static const UInt8 TABLE[] =
{
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 00..1f
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 20..3f
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 40..5f
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, // 60..7f
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9, // 80..9f
    7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7, // a0..bf
    8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, // c0..df
    0xa,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x4,0x3,0x3, // e0..ef
    0xb,0x6,0x6,0x6,0x5,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8,0x8, // f0..ff
    0x0,0x1,0x2,0x3,0x5,0x8,0x7,0x1,0x1,0x1,0x4,0x6,0x1,0x1,0x1,0x1, // s0..s0
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,0,1,0,1,1,1,1,1,1, // s1..s2
    1,2,1,1,1,1,1,2,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1, // s3..s4
    1,2,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,3,1,3,1,1,1,1,1,1, // s5..s6
    1,3,1,1,1,1,1,3,1,3,1,1,1,1,1,1,1,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1, // s7..s8
};

struct UTF8Decoder
{
    enum
    {
        ACCEPT = 0,
        REJECT = 1
    };

    UInt32 decode(UInt8 byte)
    {
        UInt32 type = TABLE[byte];
        codepoint = (state != ACCEPT) ? (byte & 0x3fu) | (codepoint << 6) : (0xff >> type) & (byte);
        state = TABLE[256 + state * 16 + type];
        return state;
    }

    void reset()
    {
        state = ACCEPT;
        codepoint = 0xfffdU;
    }

    UInt8 state {ACCEPT};
    UInt32 codepoint {0};
};

static int wcwidth(wchar_t wc)
{
    int width = widechar_wcwidth(wc);
    switch (width)
    {
        case widechar_nonprint:
            [[fallthrough]];
        case widechar_combining:
            [[fallthrough]];
        case widechar_unassigned:
            return 0;
        case widechar_ambiguous:
            [[fallthrough]];
        case widechar_private_use:
            [[fallthrough]];
        case widechar_widened_in_9:
            return 1;
        default:
            return width;
    }
}

size_t computeWidth(const UInt8 * data, size_t size, size_t prefix) noexcept
{
    UTF8Decoder decoder;
    size_t width = 0;
    size_t rollback = 0;
    for (size_t i = 0; i < size; ++i)
    {
        switch (decoder.decode(data[i]))
        {
            case UTF8Decoder::REJECT:
                decoder.reset();
                // invalid sequences seem to have zero width in modern terminals
                // tested in libvte-based, alacritty, urxvt and xterm
                i -= rollback;
                rollback = 0;
                break;
            case UTF8Decoder::ACCEPT:
                // there are special control characters that manipulate the terminal output.
                // (`0x08`, `0x09`, `0x0a`, `0x0b`, `0x0c`, `0x0d`, `0x1b`)
                // Since we don't touch the original column data, there is no easy way to escape them.
                // TODO: escape control characters
                // TODO: multiline support for '\n'

                // special treatment for '\t'
                if (decoder.codepoint == '\t')
                    width += 8 - (prefix + width) % 8;
                else
                    width += wcwidth(decoder.codepoint);
                rollback = 0;
                break;
            // continue if we meet other values here
            default:
                ++rollback;
        }
    }

    // no need to handle trailing sequence as they have zero width
    return width;
}
}
}
