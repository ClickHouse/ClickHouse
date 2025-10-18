#pragma once

#include <base/MacAddress.h>
#include <base/hex.h>
#include <cstring>
#include <cstdint>

namespace DB
{

/// Maximum text length for MAC-48 address: "FF:FF:FF:FF:FF:FF" = 17 characters
static constexpr size_t MAC_ADDRESS_MAX_TEXT_LENGTH = 17;

/// Minimum text length for MAC-48 address: "000000000000" = 12 characters (no delimiters)
static constexpr size_t MAC_ADDRESS_MIN_TEXT_LENGTH = 12;

/// Binary length for MAC-48 address in bytes
static constexpr size_t MAC_ADDRESS_BINARY_LENGTH = 6;


/**
 * Formats a MAC address to text representation.
 * Output format: lowercase colon-separated (e.g., "00:1a:2b:3c:4d:5e")
 *
 * @param src - pointer to 6-byte MAC address in network byte order (big-endian)
 * @param dst - output buffer, must have at least MAC_ADDRESS_MAX_TEXT_LENGTH bytes
 * @return pointer to the position after the last written character
 */
inline char * formatMacAddress(const unsigned char * src, char * dst)
{
    const char * hex_chars = "0123456789abcdef";

    for (size_t i = 0; i < MAC_ADDRESS_BINARY_LENGTH; ++i)
    {
        if (i > 0)
            *dst++ = ':';

        *dst++ = hex_chars[src[i] >> 4];
        *dst++ = hex_chars[src[i] & 0x0F];
    }

    return dst;
}


/**
 * Parses a MAC address from text representation.
 * Supports multiple formats:
 * - Colon-separated: "00:1A:2B:3C:4D:5E" or "00:1a:2b:3c:4d:5e"
 * - Hyphen-separated: "00-1A-2B-3C-4D-5E" or "00-1a-2b-3c-4d-5e"
 * - Dot-separated (Cisco): "001A.2B3C.4D5E" or "001a.2b3c.4d5e"
 * - Raw hexadecimal: "001A2B3C4D5E" or "001a2b3c4d5e"
 *
 * @param src - input iterator (will be advanced)
 * @param eof - function returning true if iterator reached the end
 * @param dst - output buffer for 6-byte MAC address in network byte order
 * @return true if parsed successfully, false otherwise
 */
template <typename T, typename IsEOF>
inline bool parseMacAddress(T & src, IsEOF eof, unsigned char * dst)
{
    if (eof())
        return false;

    unsigned char bytes[MAC_ADDRESS_BINARY_LENGTH];
    memset(bytes, 0, MAC_ADDRESS_BINARY_LENGTH);

    char delimiter = '\0';
    bool delimiter_found = false;
    int nibble_count = 0;
    size_t byte_index = 0;
    unsigned char current_byte = 0;
    bool high_nibble = true;

    while (!eof())
    {
        char c = *src;

        // Check for delimiters
        if (c == ':' || c == '-' || c == '.')
        {
            if (!delimiter_found)
            {
                delimiter = c;
                delimiter_found = true;
            }
            else if (c != delimiter)
            {
                return false; // Mixed delimiters
            }

            // For Cisco format (dot), we expect 4 nibbles between dots
            // For colon/hyphen, we expect 2 nibbles between delimiters
            if (delimiter == '.')
            {
                if (nibble_count != 4)
                    return false;
            }
            else
            {
                if (nibble_count != 2)
                    return false;
            }

            nibble_count = 0;
            high_nibble = true;
            ++src;
            continue;
        }

        // Parse hex digit
        UInt8 nibble = unhex(c);
        if (nibble == 0xFF)
            break; // Not a hex character, stop parsing

        if (byte_index >= MAC_ADDRESS_BINARY_LENGTH)
            return false; // Too many bytes

        if (high_nibble)
        {
            current_byte = nibble << 4;
            high_nibble = false;
        }
        else
        {
            current_byte |= nibble;
            bytes[byte_index++] = current_byte;
            current_byte = 0;
            high_nibble = true;
        }

        ++nibble_count;
        ++src;
    }

    // Check if we have a complete MAC address
    if (byte_index != MAC_ADDRESS_BINARY_LENGTH)
        return false;

    // If we had a delimiter, verify the format was consistent
    if (delimiter_found)
    {
        if (delimiter == '.')
        {
            // Cisco format: should have exactly 12 hex digits with 2 dots
            // Format: XXXX.XXXX.XXXX
            if (nibble_count != 4)
                return false;
        }
        else
        {
            // Colon or hyphen format: should have exactly 2 hex digits in last group
            if (nibble_count != 2)
                return false;
        }
    }
    else
    {
        // No delimiter: should have exactly 12 hex digits
        if (!high_nibble)
            return false; // Odd number of nibbles
    }

    // Copy to output
    memcpy(dst, bytes, MAC_ADDRESS_BINARY_LENGTH);
    return true;
}


/**
 * Parses a MAC address from a string with known boundaries.
 *
 * @param src - pointer to the start of the string
 * @param end - pointer to the end of the string
 * @param dst - output buffer for 6-byte MAC address
 * @return pointer to the position after the last parsed character, or nullptr on failure
 */
inline const char * parseMacAddress(const char * src, const char * end, unsigned char * dst)
{
    if (parseMacAddress(src, [&src, end](){ return src == end; }, dst))
        return src;

    return nullptr;
}


/**
 * Parses a MAC address from a complete string (must consume the entire string).
 *
 * @param src - pointer to the start of the string
 * @param end - pointer to the end of the string
 * @param dst - output buffer for 6-byte MAC address
 * @return true if the entire string was successfully parsed as a MAC address
 */
inline bool parseMacAddressWhole(const char * src, const char * end, unsigned char * dst)
{
    return parseMacAddress(src, end, dst) == end;
}

}

