#pragma once

/// This allows building Lexer without any dependencies or includes for WebAssembly or Emscripten.

#define chassert(...)

#define size_t unsigned long
#define uint8_t unsigned char
#ifndef __cpp_char8_t
    #define char8_t unsigned char
#endif


namespace // NOLINT
{

template <char... symbols>
inline const char * find_first_symbols(const char * begin, const char * end)
{
    for (; begin != end; ++begin)
        if (((*begin == symbols) || ...))
            break;
    return begin;
}

template <char... symbols>
inline const char * find_first_not_symbols(const char * begin, const char * end)
{
    for (; begin != end; ++begin)
        if (((*begin != symbols) && ...))
            break;
    return begin;
}

inline bool isNumericASCII(char c)
{
    return (c >= '0' && c <= '9');
}

inline bool isLowerAlphaASCII(char c)
{
    return (c >= 'a' && c <= 'z');
}

inline bool isUpperAlphaASCII(char c)
{
    return (c >= 'A' && c <= 'Z');
}

inline bool isAlphaASCII(char c)
{
    return isLowerAlphaASCII(c) || isUpperAlphaASCII(c);
}

inline bool isAlphaNumericASCII(char c)
{
    return isAlphaASCII(c)
           || isNumericASCII(c);
}

inline bool isHexDigit(char c)
{
    return isNumericASCII(c)
           || (c >= 'a' && c <= 'f')
           || (c >= 'A' && c <= 'F');
}

inline bool isNumberSeparator(bool is_start_of_block, bool is_hex, const char * pos, const char * end)
{
    if (*pos != '_')
        return false;
    if (is_start_of_block && *pos == '_')
        return false; // e.g. _123, 12e_3
    if (pos + 1 < end && !(is_hex ? isHexDigit(pos[1]) : isNumericASCII(pos[1])))
        return false; // e.g. 1__2, 1_., 1_e, 1_p, 1_;
    if (pos + 1 == end)
        return false; // e.g. 12_
    return true;
}

inline bool isWhitespaceASCII(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

inline bool isWordCharASCII(char c)
{
    return isAlphaNumericASCII(c)
           || c == '_';
}

namespace std
{
namespace string
{
constexpr size_t npos = -1;
}

struct string_view
{
    const char * ptr;
    size_t size;

    string_view(const char * ptr_, size_t size_) : ptr(ptr_), size(size_)
    {
    }

    string_view(const char * begin, const char * end) : ptr(begin), size(end - begin)
    {
    }

    inline size_t find(string_view other) const
    {
        for (size_t offset = 0; offset + other.size <= size; ++offset)
        {
            size_t other_pos = 0;
            for (; other_pos < other.size; ++other_pos)
                if (ptr[offset + other_pos] != other.ptr[other_pos])
                    break;
            if (other_pos == other.size)
                return offset;
        }
        return string::npos;
    }
};
}

inline const char * skipWhitespacesUTF8(const char * pos, const char * end)
{
    while (pos < end)
    {
        if (isWhitespaceASCII(*pos))
        {
            ++pos;
        }
        else
        {
            const uint8_t * upos = reinterpret_cast<const uint8_t *>(pos);

            if (pos + 1 < end && upos[0] == 0xC2 && (upos[1] == 0x85 || upos[1] == 0xA0))
            {
                pos += 2;
            }
            else if (pos + 2 < end
                     &&    ((upos[0] == 0xE1 && upos[1] == 0xA0 && upos[2] == 0x8E)
                            || (upos[0] == 0xE2
                                &&    ((upos[1] == 0x80
                                        &&    ((upos[2] >= 0x80 && upos[2] <= 0x8A)
                                               || (upos[2] >= 0xA8 && upos[2] <= 0xA9)
                                               || (upos[2] >= 0x8B && upos[2] <= 0x8D)
                                               || (upos[2] == 0xAF)))
                                       || (upos[1] == 0x81 && (upos[2] == 0x9F || upos[2] == 0xA0))))
                            || (upos[0] == 0xE3 && upos[1] == 0x80 && upos[2] == 0x80)
                            || (upos[0] == 0xEF && upos[1] == 0xBB && upos[2] == 0xBF)))
            {
                pos += 3;
            }
            else
                break;
        }
    }

    return pos;
}

namespace UTF8
{
inline bool isContinuationOctet(char8_t octet)
{
    return (octet & 0b11000000u) == 0b10000000u;
}
}

}

inline void * operator new(size_t, void * p) noexcept { return p; }
