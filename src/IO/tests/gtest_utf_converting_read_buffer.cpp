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
    utf16_input.push_back(static_cast<char>(0xFF));
    utf16_input.push_back(static_cast<char>(0xFE)); // BOM
    
    // Fill with enough dummy characters to exceed DBMS_DEFAULT_BUFFER_SIZE
    // We want the output buffer to run out of space EXACTLY after high1 is read.
    // DBMS_DEFAULT_BUFFER_SIZE is 1048576 bytes.
    // Each ASCII character ('A') is 2 bytes in UTF-16LE and 1 byte in UTF-8.
    // So 1048576 characters = 2097152 bytes in UTF-16LE, 1048576 bytes in UTF-8.
    
    // We need exactly (1048576 - 6) bytes to be filled, so that output_ptr + 6 <= output_end is false
    // AFTER reading high1, BUT high1 shouldn't be written.
    
    size_t dummy_chars = 1048576 - 6; 
    
    for (size_t i = 0; i < dummy_chars; ++i)
    {
        utf16_input.push_back('A');
        utf16_input.push_back('\0');
    }
    
    utf16_input.push_back('\0');
    utf16_input.push_back(static_cast<char>(0xD8)); // high1
    utf16_input.push_back('\x01');
    utf16_input.push_back(static_cast<char>(0xD8)); // high2
    utf16_input.push_back('\0');
    utf16_input.push_back(static_cast<char>(0xDC)); // low2
    
    auto in = std::make_unique<ReadBufferFromString>(utf16_input);
    UTFConvertingReadBuffer utf_in(std::move(in));
    
    String utf8_output;
    WriteBufferFromString out(utf8_output);
    copyData(utf_in, out);
    out.finalize();
    
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

TEST(UTFConvertingReadBuffer, UTF8Passthrough)
{
    // Without BOM
    String input = "Hello, world!";
    auto in = std::make_unique<ReadBufferFromString>(input);
    UTFConvertingReadBuffer utf_in(std::move(in));
    String output;
    WriteBufferFromString out(output);
    copyData(utf_in, out);
    out.finalize();
    EXPECT_EQ(output, "Hello, world!");
    EXPECT_EQ(utf_in.getEncoding(), UTFConvertingReadBuffer::Encoding::UTF8);

    // With UTF-8 BOM
    String input_bom = "\xEF\xBB\xBFHello, world!";
    auto in_bom = std::make_unique<ReadBufferFromString>(input_bom);
    UTFConvertingReadBuffer utf_in_bom(std::move(in_bom));
    String output_bom;
    WriteBufferFromString out_bom(output_bom);
    copyData(utf_in_bom, out_bom);
    out_bom.finalize();
    EXPECT_EQ(output_bom, "Hello, world!");
    EXPECT_EQ(utf_in_bom.getEncoding(), UTFConvertingReadBuffer::Encoding::UTF8);
}

TEST(UTFConvertingReadBuffer, UTF16LEAndBEConversion)
{
    // UTF-16LE: "AB" = 0xFF 0xFE 'A' 0x00 'B' 0x00
    String input_le;
    input_le.push_back(static_cast<char>(0xFF));
    input_le.push_back(static_cast<char>(0xFE));
    input_le.push_back('A');
    input_le.push_back('\0');
    input_le.push_back('B');
    input_le.push_back('\0');

    auto in_le = std::make_unique<ReadBufferFromString>(input_le);
    UTFConvertingReadBuffer utf_in_le(std::move(in_le));
    String output_le;
    WriteBufferFromString out_le(output_le);
    copyData(utf_in_le, out_le);
    out_le.finalize();
    EXPECT_EQ(output_le, "AB");
    EXPECT_EQ(utf_in_le.getEncoding(), UTFConvertingReadBuffer::Encoding::UTF16_LE);

    // UTF-16BE: "AB" = 0xFE 0xFF 0x00 'A' 0x00 'B'
    String input_be;
    input_be.push_back(static_cast<char>(0xFE));
    input_be.push_back(static_cast<char>(0xFF));
    input_be.push_back('\0');
    input_be.push_back('A');
    input_be.push_back('\0');
    input_be.push_back('B');

    auto in_be = std::make_unique<ReadBufferFromString>(input_be);
    UTFConvertingReadBuffer utf_in_be(std::move(in_be));
    String output_be;
    WriteBufferFromString out_be(output_be);
    copyData(utf_in_be, out_be);
    out_be.finalize();
    EXPECT_EQ(output_be, "AB");
    EXPECT_EQ(utf_in_be.getEncoding(), UTFConvertingReadBuffer::Encoding::UTF16_BE);
}

TEST(UTFConvertingReadBuffer, UTF32LEAndBEConversion)
{
    // UTF-32LE: "AB" = 0xFF 0xFE 0x00 0x00 'A' 0x00 0x00 0x00 'B' 0x00 0x00 0x00
    String input_le;
    input_le.push_back(static_cast<char>(0xFF));
    input_le.push_back(static_cast<char>(0xFE));
    input_le.push_back('\0');
    input_le.push_back('\0');
    input_le.push_back('A');
    input_le.push_back('\0');
    input_le.push_back('\0');
    input_le.push_back('\0');
    input_le.push_back('B');
    input_le.push_back('\0');
    input_le.push_back('\0');
    input_le.push_back('\0');

    auto in_le = std::make_unique<ReadBufferFromString>(input_le);
    UTFConvertingReadBuffer utf_in_le(std::move(in_le));
    String output_le;
    WriteBufferFromString out_le(output_le);
    copyData(utf_in_le, out_le);
    out_le.finalize();
    EXPECT_EQ(output_le, "AB");
    EXPECT_EQ(utf_in_le.getEncoding(), UTFConvertingReadBuffer::Encoding::UTF32_LE);

    // UTF-32BE: "AB" = 0x00 0x00 0xFE 0xFF 0x00 0x00 0x00 'A' 0x00 0x00 0x00 'B'
    String input_be;
    input_be.push_back('\0');
    input_be.push_back('\0');
    input_be.push_back(static_cast<char>(0xFE));
    input_be.push_back(static_cast<char>(0xFF));
    input_be.push_back('\0');
    input_be.push_back('\0');
    input_be.push_back('\0');
    input_be.push_back('A');
    input_be.push_back('\0');
    input_be.push_back('\0');
    input_be.push_back('\0');
    input_be.push_back('B');

    auto in_be = std::make_unique<ReadBufferFromString>(input_be);
    UTFConvertingReadBuffer utf_in_be(std::move(in_be));
    String output_be;
    WriteBufferFromString out_be(output_be);
    copyData(utf_in_be, out_be);
    out_be.finalize();
    EXPECT_EQ(output_be, "AB");
    EXPECT_EQ(utf_in_be.getEncoding(), UTFConvertingReadBuffer::Encoding::UTF32_BE);
}

TEST(UTFConvertingReadBuffer, FailCloseOnInvalidSequences)
{
    // UTF-16LE with unpaired high surrogate 0xD800 followed by 'A'
    String invalid_input;
    invalid_input.push_back(static_cast<char>(0xFF));
    invalid_input.push_back(static_cast<char>(0xFE));
    invalid_input.push_back('\0');
    invalid_input.push_back(static_cast<char>(0xD8));
    invalid_input.push_back('A');
    invalid_input.push_back('\0');

    // With replace_invalid_sequences = false, it should throw DB::Exception
    auto in_strict = std::make_unique<ReadBufferFromString>(invalid_input);
    UTFConvertingReadBuffer utf_strict(std::move(in_strict), /*replace_invalid_sequences=*/false);
    String output;
    WriteBufferFromString out(output);
    EXPECT_THROW(copyData(utf_strict, out), DB::Exception);

    // With replace_invalid_sequences = true, it should replace with U+FFFD
    auto in_replace = std::make_unique<ReadBufferFromString>(invalid_input);
    UTFConvertingReadBuffer utf_replace(std::move(in_replace), /*replace_invalid_sequences=*/true);
    String output_replace;
    WriteBufferFromString out_replace(output_replace);
    EXPECT_NO_THROW(copyData(utf_replace, out_replace));
    out_replace.finalize();
    EXPECT_NE(output_replace.find("\xEF\xBF\xBD"), String::npos);
}
