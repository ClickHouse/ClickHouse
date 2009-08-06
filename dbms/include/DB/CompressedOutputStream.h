#ifndef DBMS_COMPRESSED_OUTPUT_STREAM_H
#define DBMS_COMPRESSED_OUTPUT_STREAM_H

#include <istream>
#include <ostream>
#include <vector>

#include <Poco/BufferedStreamBuf.h>

#include <quicklz/quicklz_level1.h>

#include <DB/CompressedStream.h>


namespace DB
{


/** Аналогично Poco::DeflatingStreamBuf, но используется библиотека QuickLZ,
  * а также поддерживается только ostream.
  */
class CompressingStreamBuf : public Poco::BufferedStreamBuf
{
public:
	CompressingStreamBuf(std::ostream & ostr);
	~CompressingStreamBuf();
	int close();

protected:
	int writeToDevice(const char * buffer, std::streamsize length);

private:
	size_t pos_in_buffer;
	std::ostream * p_ostr;
	std::vector<char> uncompressed_buffer;
	std::vector<char> compressed_buffer;
	std::vector<char> scratch;

	/** Сжимает данные, находящиеся в буфере и записывает их. */
	void writeCompressedChunk();
};


/** Базовый класс для CompressedOutputStream; содержит CompressingStreamBuf
  */
class CompressingIOS : public virtual std::ios
{
public:
	CompressingIOS(std::ostream & ostr);
	CompressingStreamBuf * rdbuf();

protected:
	CompressingStreamBuf buf;
};


/** Сжимает всё с помощью алгоритма QuickLZ блоками не более DBMS_COMPRESSING_STREAM_BUFFER_SIZE.
  * Для записи последнего блока, следует вызвать метод close().
  */
class CompressedOutputStream : public CompressingIOS, public std::ostream
{
public:
	CompressedOutputStream(std::ostream & ostr);
	int close();
};


}


#endif
