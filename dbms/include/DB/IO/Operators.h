#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/** Реализует возможность записывать и считывать данные в WriteBuffer/ReadBuffer
  *  с помощью операторов << и >> а также манипуляторов,
  *  предоставляя способ использования, похожий на iostream-ы.
  *
  * Не является ни подмножеством, ни расширением iostream-ов.
  *
  * Пример использования:
  *
  *     DB::WriteBufferFromFileDescriptor buf(STDOUT_FILENO);
  *     buf << DB::double_quote << "Hello, world!" << '\n' << DB::flush;
  *
  * Выводит тип char (как правило, он же Int8) как символ, а не как число.
  */

/// Манипуляторы.
enum EscapeManip 		{ escape };			/// Для строк - экранировать спец-символы. В остальном, как обычно.
enum QuoteManip 		{ quote };			/// Для строк, дат, дат-с-временем - заключить в одинарные кавычки с экранированием. В остальном, как обычно.
enum DoubleQuoteManip 	{ double_quote };	/// Для строк, дат, дат-с-временем - заключить в двойные кавычки с экранированием. В остальном, как обычно.
enum BinaryManip 		{ binary };			/// Выводить в бинарном формате.

struct EscapeManipWriteBuffer 		: WriteBuffer {};
struct QuoteManipWriteBuffer 		: WriteBuffer {};
struct DoubleQuoteManipWriteBuffer 	: WriteBuffer {};
struct BinaryManipWriteBuffer 		: WriteBuffer {};

struct EscapeManipReadBuffer 		: ReadBuffer {};
struct QuoteManipReadBuffer 		: ReadBuffer {};
struct DoubleQuoteManipReadBuffer 	: ReadBuffer {};
struct BinaryManipReadBuffer 		: ReadBuffer {};


template <typename T> 	WriteBuffer & operator<< (WriteBuffer & buf, const T & x) 		{ writeText(x, buf); 	return buf; }
/// Если не использовать манипуляторы, строка выводится без экранирования, как есть.
template <> inline		WriteBuffer & operator<< (WriteBuffer & buf, const String & x) 	{ writeString(x, buf); 	return buf; }
template <> inline		WriteBuffer & operator<< (WriteBuffer & buf, const char & x) 	{ writeChar(x, buf); 	return buf; }

inline WriteBuffer & operator<< (WriteBuffer & buf, const char * x) 	{ writeCString(x, buf); return buf; }

inline EscapeManipWriteBuffer &		operator<< (WriteBuffer & buf, EscapeManip x) 		{ return static_cast<EscapeManipWriteBuffer &>(buf); }
inline QuoteManipWriteBuffer &		operator<< (WriteBuffer & buf, QuoteManip x) 		{ return static_cast<QuoteManipWriteBuffer &>(buf); }
inline DoubleQuoteManipWriteBuffer & operator<< (WriteBuffer & buf, DoubleQuoteManip x) { return static_cast<DoubleQuoteManipWriteBuffer &>(buf); }
inline BinaryManipWriteBuffer &		operator<< (WriteBuffer & buf, BinaryManip x) 		{ return static_cast<BinaryManipWriteBuffer &>(buf); }

template <typename T> WriteBuffer & operator<< (EscapeManipWriteBuffer & buf, 		const T & x) { writeText(x, buf); 			return buf; }
template <typename T> WriteBuffer & operator<< (QuoteManipWriteBuffer & buf, 		const T & x) { writeQuoted(x, buf); 		return buf; }
template <typename T> WriteBuffer & operator<< (DoubleQuoteManipWriteBuffer & buf, 	const T & x) { writeDoubleQuoted(x, buf); 	return buf; }
template <typename T> WriteBuffer & operator<< (BinaryManipWriteBuffer & buf, 		const T & x) { writeBinary(x, buf); 		return buf; }

inline WriteBuffer & operator<< (EscapeManipWriteBuffer & buf, 		const char * x) { writeAnyEscapedString<'\''>(x, x + strlen(x), buf); return buf; }
inline WriteBuffer & operator<< (QuoteManipWriteBuffer & buf, 		const char * x) { writeAnyQuotedString<'\''>(x, x + strlen(x), buf); return buf; }
inline WriteBuffer & operator<< (DoubleQuoteManipWriteBuffer & buf, const char * x) { writeAnyQuotedString<'"'>(x, x + strlen(x), buf); return buf; }
inline WriteBuffer & operator<< (BinaryManipWriteBuffer & buf, 		const char * x) { writeStringBinary(x, buf); return buf; }

/// Манипулятор вызывает у WriteBuffer метод next - это делает сброс буфера. Для вложенных буферов, сброс не рекурсивный.
enum FlushManip { flush };

inline WriteBuffer & operator<< (WriteBuffer & buf, FlushManip x) { buf.next(); return buf; }


template <typename T> 		ReadBuffer & operator>> (ReadBuffer & buf, T & x) 				{ readText(x, buf); 	return buf; }
template <> inline 			ReadBuffer & operator>> (ReadBuffer & buf, String & x) 			{ readString(x, buf); 	return buf; }
template <> inline 			ReadBuffer & operator>> (ReadBuffer & buf, char & x) 			{ readChar(x, buf); 	return buf; }

/// Если указать для чтения строковый литерал, то это будет обозначать - убедиться в наличии последовательности байт и пропустить её.
inline ReadBuffer & operator>> (ReadBuffer & buf, const char * x) 	{ assertString(x, buf); return buf; }

inline EscapeManipReadBuffer &		operator>> (ReadBuffer & buf, EscapeManip x) 		{ return static_cast<EscapeManipReadBuffer &>(buf); }
inline QuoteManipReadBuffer &		operator>> (ReadBuffer & buf, QuoteManip x) 		{ return static_cast<QuoteManipReadBuffer &>(buf); }
inline DoubleQuoteManipReadBuffer &	operator>> (ReadBuffer & buf, DoubleQuoteManip x) 	{ return static_cast<DoubleQuoteManipReadBuffer &>(buf); }
inline BinaryManipReadBuffer &		operator>> (ReadBuffer & buf, BinaryManip x) 		{ return static_cast<BinaryManipReadBuffer &>(buf); }

template <typename T> ReadBuffer & operator>> (EscapeManipReadBuffer & buf, 		T & x) { readText(x, buf); 			return buf; }
template <typename T> ReadBuffer & operator>> (QuoteManipReadBuffer & buf, 			T & x) { readQuoted(x, buf); 		return buf; }
template <typename T> ReadBuffer & operator>> (DoubleQuoteManipReadBuffer & buf, 	T & x) { readDoubleQuoted(x, buf); 	return buf; }
template <typename T> ReadBuffer & operator>> (BinaryManipReadBuffer & buf, 		T & x) { readBinary(x, buf); 		return buf; }

}
