#include <gtest/gtest.h>
#include <IO/UTFConvertingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

using namespace DB;

TEST(UTFConvertingReadBuffer, SurrogateBoundary)
{
    // Test for malformed sequence spanning chunk boundary
    // Sequence: high1, high2, low2
    // We expect output to be REPLACEMENT_CHARACTER + decoded(high2, low2).
    
    // In UTF-16LE:
    // high1 = 0xD800 (bytes: 0x00 0xD8)
    // high2 = 0xD801 (bytes: 0x01 0xD8)
    // low2  = 0xDC00 (bytes: 0x00 0xDC)
    
    String utf16_input;
    utf16_input += "\xFF\xFE"; // BOM
    
    // Fill with enough dummy characters to exceed DBMS_DEFAULT_BUFFER_SIZE
    // We want the output buffer to run out of space EXACTLY after high1 is read.
    // DBMS_DEFAULT_BUFFER_SIZE is 1048576 bytes.
    // Each ASCII character ('A') is 2 bytes in UTF-16LE and 1 byte in UTF-8.
    // So 1048576 characters = 2097152 bytes in UTF-16LE, 1048576 bytes in UTF-8.
    
    // We need exactly (1048576 - 6) bytes to be filled, so that output_ptr + 6 <= output_end is false
    // AFTER reading high1, BUT high1 shouldn't be written.
    
    size_t dummy_chars = 1048576 - 3; 
    
    for (size_t i = 0; i < dummy_chars; ++i)
    {
        utf16_input += "A\x00"; // 'A' in UTF-16LE
    }
    
    utf16_input += "\x00\xD8"; // high1
    utf16_input += "\x01\xD8"; // high2
    utf16_input += "\x00\xDC"; // low2
    
    auto in = std::make_unique<ReadBufferFromString>(utf16_input);
    UTFConvertingReadBuffer utf_in(std::move(in));
    
    String utf8_output;
    WriteBufferFromString out(utf8_output);
    copyData(utf_in, out);
    
    // Verify the end of the output string
    // It should be 'A' followed by U+FFFD (replacement character) and U+10400
    // REPLACEMENT_CHARACTER in UTF-8 is \xEF\xBF\xBD
    // U+10400 in UTF-8 is \xF0\x90\x90\x80
    
    String expected_end;
    expected_end += "A";
    expected_end += "\xEF\xBF\xBD"; // U+FFFD
    expected_end += "\xF0\x90\x90\x80"; // U+10400
    
    ASSERT_GT(utf8_output.size(), expected_end.size());
    EXPECT_EQ(utf8_output.substr(utf8_output.size() - expected_end.size()), expected_end);
}
