#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/** Implements the ability to write and read data in/from WriteBuffer/ReadBuffer
  *  with the help of << and >> operators and also manipulators,
  *  providing a way of using, similar to iostreams.
  *
  * It is neither a subset nor an extension of iostreams.
  *
  * Example usage:
  *
  *     DB::WriteBufferFromFileDescriptor buf(STDOUT_FILENO);
  *     buf << DB::double_quote << "Hello, world!" << '\n' << DB::flush;
  *
  * Outputs `char` type (usually it's Int8) as a symbol, not as a number.
  */

/// Manipulators.
enum EscapeManip        { escape };           /// For strings - escape special characters. In the rest, as usual.
enum QuoteManip         { quote };            /// For strings, dates, datetimes - enclose in single quotes with escaping. In the rest, as usual.
enum DoubleQuoteManip   { double_quote };     /// For strings, dates, datetimes - enclose in double quotes with escaping. In the rest, as usual.
enum BinaryManip        { binary };           /// Output in binary format.
enum XMLManip           { xml };              /// Output strings with XML escaping.

struct EscapeManipWriteBuffer        : std::reference_wrapper<WriteBuffer> { using std::reference_wrapper<WriteBuffer>::reference_wrapper; };
struct QuoteManipWriteBuffer         : std::reference_wrapper<WriteBuffer> { using std::reference_wrapper<WriteBuffer>::reference_wrapper; };
struct DoubleQuoteManipWriteBuffer   : std::reference_wrapper<WriteBuffer> { using std::reference_wrapper<WriteBuffer>::reference_wrapper; };
struct BinaryManipWriteBuffer        : std::reference_wrapper<WriteBuffer> { using std::reference_wrapper<WriteBuffer>::reference_wrapper; };
struct XMLManipWriteBuffer           : std::reference_wrapper<WriteBuffer> { using std::reference_wrapper<WriteBuffer>::reference_wrapper; };

struct EscapeManipReadBuffer         : std::reference_wrapper<ReadBuffer> { using std::reference_wrapper<ReadBuffer>::reference_wrapper; };
struct QuoteManipReadBuffer          : std::reference_wrapper<ReadBuffer> { using std::reference_wrapper<ReadBuffer>::reference_wrapper; };
struct DoubleQuoteManipReadBuffer    : std::reference_wrapper<ReadBuffer> { using std::reference_wrapper<ReadBuffer>::reference_wrapper; };
struct BinaryManipReadBuffer         : std::reference_wrapper<ReadBuffer> { using std::reference_wrapper<ReadBuffer>::reference_wrapper; };

inline WriteBuffer & operator<<(WriteBuffer & buf, const auto & x) { writeText(x, buf); return buf; }

struct PcgSerializer
{
    static void serializePcg32(const pcg32_fast & rng, WriteBuffer & buf)
    {
        writeText(pcg32_fast::multiplier(), buf);
        writeChar(' ', buf);
        writeText(pcg32_fast::increment(), buf);
        writeChar(' ', buf);
        writeText(rng.state_, buf);
    }
};

inline WriteBuffer & operator<<(WriteBuffer & buf, const pcg32_fast & x) { PcgSerializer::serializePcg32(x, buf); return buf; }

inline EscapeManipWriteBuffer      operator<< (WriteBuffer & buf, EscapeManip)      { return buf; }
inline QuoteManipWriteBuffer       operator<< (WriteBuffer & buf, QuoteManip)       { return buf; }
inline DoubleQuoteManipWriteBuffer operator<< (WriteBuffer & buf, DoubleQuoteManip) { return buf; }
inline BinaryManipWriteBuffer      operator<< (WriteBuffer & buf, BinaryManip)      { return buf; }
inline XMLManipWriteBuffer         operator<< (WriteBuffer & buf, XMLManip)         { return buf; }

template <typename T> WriteBuffer & operator<< (EscapeManipWriteBuffer buf,        const T & x) { writeText(x, buf.get());         return buf; }
template <typename T> WriteBuffer & operator<< (QuoteManipWriteBuffer buf,         const T & x) { writeQuoted(x, buf.get());       return buf; }
template <typename T> WriteBuffer & operator<< (DoubleQuoteManipWriteBuffer buf,   const T & x) { writeDoubleQuoted(x, buf.get()); return buf; }
template <typename T> WriteBuffer & operator<< (BinaryManipWriteBuffer buf,        const T & x) { writeBinary(x, buf.get());       return buf; }
template <typename T> WriteBuffer & operator<< (XMLManipWriteBuffer buf,           const T & x) { writeText(x, buf.get());         return buf; }

inline  WriteBuffer & operator<< (EscapeManipWriteBuffer buf, const String & x)   { writeEscapedString(x, buf); return buf; }
inline  WriteBuffer & operator<< (EscapeManipWriteBuffer buf, std::string_view x) { writeEscapedString(x, buf); return buf; }
inline  WriteBuffer & operator<< (EscapeManipWriteBuffer buf, StringRef x)        { writeEscapedString(x.toView(), buf); return buf; }
inline  WriteBuffer & operator<< (EscapeManipWriteBuffer buf, const char * x)     { writeEscapedString(x, strlen(x), buf); return buf; }

inline WriteBuffer & operator<< (QuoteManipWriteBuffer buf,       const char * x) { writeAnyQuotedString<'\''>(x, x + strlen(x), buf.get()); return buf; }
inline WriteBuffer & operator<< (DoubleQuoteManipWriteBuffer buf, const char * x) { writeAnyQuotedString<'"'>(x, x + strlen(x), buf.get()); return buf; }
inline WriteBuffer & operator<< (BinaryManipWriteBuffer buf,      const char * x) { writeStringBinary(x, buf.get()); return buf; }

inline  WriteBuffer & operator<< (XMLManipWriteBuffer buf, std::string_view x) { writeXMLStringForTextElementOrAttributeValue(x, buf); return buf; }
inline  WriteBuffer & operator<< (XMLManipWriteBuffer buf, StringRef x)        { writeXMLStringForTextElementOrAttributeValue(x.toView(), buf); return buf; }
inline  WriteBuffer & operator<< (XMLManipWriteBuffer buf, const char * x)     { writeXMLStringForTextElementOrAttributeValue(std::string_view(x), buf); return buf; }

/// The manipulator calls the WriteBuffer method `next` - this makes the buffer reset. For nested buffers, the reset is not recursive.
enum FlushManip { flush };

inline WriteBuffer & operator<< (WriteBuffer & buf, FlushManip) { buf.next(); return buf; }


template <typename T> ReadBuffer & operator>> (ReadBuffer & buf, T & x)              { readText(x, buf);     return buf; }
template <> inline    ReadBuffer & operator>> (ReadBuffer & buf, String & x)         { readString(x, buf);   return buf; }
template <> inline    ReadBuffer & operator>> (ReadBuffer & buf, char & x)           { readChar(x, buf);     return buf; }

struct PcgDeserializer
{
    static void deserializePcg32(pcg32_fast & rng, ReadBuffer & buf)
    {
        decltype(rng.state_) multiplier, increment, state;
        readText(multiplier, buf);
        assertChar(' ', buf);
        readText(increment, buf);
        assertChar(' ', buf);
        readText(state, buf);

        if (multiplier != pcg32_fast::multiplier())
          throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect multiplier in pcg32: expected {}, got {}", pcg32_fast::multiplier(), multiplier);
        if (increment != pcg32_fast::increment())
          throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect increment in pcg32: expected {}, got {}", pcg32_fast::increment(), increment);

        rng.state_ = state;
    }
};

template <> inline    ReadBuffer & operator>> (ReadBuffer & buf, pcg32_fast & x)     { PcgDeserializer::deserializePcg32(x, buf); return buf; }

/// If you specify a string literal for reading, this will mean - make sure there is a sequence of bytes and skip it.
inline ReadBuffer & operator>> (ReadBuffer & buf, const char * x)     { assertString(x, buf); return buf; }

inline EscapeManipReadBuffer      operator>> (ReadBuffer & buf, EscapeManip)      { return buf; }
inline QuoteManipReadBuffer       operator>> (ReadBuffer & buf, QuoteManip)       { return buf; }
inline DoubleQuoteManipReadBuffer operator>> (ReadBuffer & buf, DoubleQuoteManip) { return buf; }
inline BinaryManipReadBuffer      operator>> (ReadBuffer & buf, BinaryManip)      { return buf; }

template <typename T> ReadBuffer & operator>> (EscapeManipReadBuffer buf,      T & x) { readText(x, buf.get());         return buf; }
template <typename T> ReadBuffer & operator>> (QuoteManipReadBuffer buf,       T & x) { readQuoted(x, buf.get());       return buf; }
template <typename T> ReadBuffer & operator>> (DoubleQuoteManipReadBuffer buf, T & x) { readDoubleQuoted(x, buf.get()); return buf; }
template <typename T> ReadBuffer & operator>> (BinaryManipReadBuffer buf,      T & x) { readBinary(x, buf.get());       return buf; }

}
