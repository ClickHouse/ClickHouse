#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

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

struct EscapeManipWriteBuffer        : WriteBuffer {};
struct QuoteManipWriteBuffer         : WriteBuffer {};
struct DoubleQuoteManipWriteBuffer   : WriteBuffer {};
struct BinaryManipWriteBuffer        : WriteBuffer {};

struct EscapeManipReadBuffer         : ReadBuffer {};
struct QuoteManipReadBuffer          : ReadBuffer {};
struct DoubleQuoteManipReadBuffer    : ReadBuffer {};
struct BinaryManipReadBuffer         : ReadBuffer {};


template <typename T>     WriteBuffer & operator<< (WriteBuffer & buf, const T & x)        { writeText(x, buf);     return buf; }
/// If you do not use the manipulators, the string is displayed without an escape, as is.
template <> inline        WriteBuffer & operator<< (WriteBuffer & buf, const String & x)   { writeString(x, buf);   return buf; }
template <> inline        WriteBuffer & operator<< (WriteBuffer & buf, const char & x)     { writeChar(x, buf);     return buf; }

inline WriteBuffer & operator<< (WriteBuffer & buf, const char * x)     { writeCString(x, buf); return buf; }

inline EscapeManipWriteBuffer &      operator<< (WriteBuffer & buf, EscapeManip x)      { return static_cast<EscapeManipWriteBuffer &>(buf); }
inline QuoteManipWriteBuffer &       operator<< (WriteBuffer & buf, QuoteManip x)       { return static_cast<QuoteManipWriteBuffer &>(buf); }
inline DoubleQuoteManipWriteBuffer & operator<< (WriteBuffer & buf, DoubleQuoteManip x) { return static_cast<DoubleQuoteManipWriteBuffer &>(buf); }
inline BinaryManipWriteBuffer &      operator<< (WriteBuffer & buf, BinaryManip x)      { return static_cast<BinaryManipWriteBuffer &>(buf); }

template <typename T> WriteBuffer & operator<< (EscapeManipWriteBuffer & buf,        const T & x) { writeText(x, buf);         return buf; }
template <typename T> WriteBuffer & operator<< (QuoteManipWriteBuffer & buf,         const T & x) { writeQuoted(x, buf);       return buf; }
template <typename T> WriteBuffer & operator<< (DoubleQuoteManipWriteBuffer & buf,   const T & x) { writeDoubleQuoted(x, buf); return buf; }
template <typename T> WriteBuffer & operator<< (BinaryManipWriteBuffer & buf,        const T & x) { writeBinary(x, buf);       return buf; }

inline WriteBuffer & operator<< (EscapeManipWriteBuffer & buf,      const char * x) { writeAnyEscapedString<'\''>(x, x + strlen(x), buf); return buf; }
inline WriteBuffer & operator<< (QuoteManipWriteBuffer & buf,       const char * x) { writeAnyQuotedString<'\''>(x, x + strlen(x), buf); return buf; }
inline WriteBuffer & operator<< (DoubleQuoteManipWriteBuffer & buf, const char * x) { writeAnyQuotedString<'"'>(x, x + strlen(x), buf); return buf; }
inline WriteBuffer & operator<< (BinaryManipWriteBuffer & buf,      const char * x) { writeStringBinary(x, buf); return buf; }

/// The manipulator calls the WriteBuffer method `next` - this makes the buffer reset. For nested buffers, the reset is not recursive.
enum FlushManip { flush };

inline WriteBuffer & operator<< (WriteBuffer & buf, FlushManip x) { buf.next(); return buf; }


template <typename T> ReadBuffer & operator>> (ReadBuffer & buf, T & x)              { readText(x, buf);     return buf; }
template <> inline    ReadBuffer & operator>> (ReadBuffer & buf, String & x)         { readString(x, buf);   return buf; }
template <> inline    ReadBuffer & operator>> (ReadBuffer & buf, char & x)           { readChar(x, buf);     return buf; }

/// If you specify a string literal for reading, this will mean - make sure there is a sequence of bytes and skip it.
inline ReadBuffer & operator>> (ReadBuffer & buf, const char * x)     { assertString(x, buf); return buf; }

inline EscapeManipReadBuffer &       operator>> (ReadBuffer & buf, EscapeManip x)      { return static_cast<EscapeManipReadBuffer &>(buf); }
inline QuoteManipReadBuffer &        operator>> (ReadBuffer & buf, QuoteManip x)       { return static_cast<QuoteManipReadBuffer &>(buf); }
inline DoubleQuoteManipReadBuffer &  operator>> (ReadBuffer & buf, DoubleQuoteManip x) { return static_cast<DoubleQuoteManipReadBuffer &>(buf); }
inline BinaryManipReadBuffer &       operator>> (ReadBuffer & buf, BinaryManip x)      { return static_cast<BinaryManipReadBuffer &>(buf); }

template <typename T> ReadBuffer & operator>> (EscapeManipReadBuffer & buf,      T & x) { readText(x, buf);         return buf; }
template <typename T> ReadBuffer & operator>> (QuoteManipReadBuffer & buf,       T & x) { readQuoted(x, buf);       return buf; }
template <typename T> ReadBuffer & operator>> (DoubleQuoteManipReadBuffer & buf, T & x) { readDoubleQuoted(x, buf); return buf; }
template <typename T> ReadBuffer & operator>> (BinaryManipReadBuffer & buf,      T & x) { readBinary(x, buf);       return buf; }

}
