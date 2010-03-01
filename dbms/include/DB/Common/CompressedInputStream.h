#ifndef DBMS_COMMON_COMPRESSED_INPUT_STREAM_H
#define DBMS_COMMON_COMPRESSED_INPUT_STREAM_H

#include <istream>
#include <ostream>
#include <vector>

#include <Poco/BufferedStreamBuf.h>

#include <quicklz/quicklz_level1.h>

#include <DB/Common/CompressedStream.h>


namespace DB
{


/** Аналогично Poco::InflatingStreamBuf, но используется библиотека QuickLZ,
  * а также поддерживается только istream.
  */
class DecompressingStreamBuf : public Poco::BufferedStreamBuf
{
public:
	DecompressingStreamBuf(std::istream & istr);

	/** прочитать целиком один сжатый блок данных;
	  */
	void getChunk(std::vector<char> & res);

protected:
	int readFromDevice(char * buffer, std::streamsize length);

private:
	size_t pos_in_buffer;
	std::istream * p_istr;
	std::vector<char> uncompressed_buffer;
	std::vector<char> compressed_buffer;
	std::vector<char> scratch;

	/** Читает и разжимает следующий кусок сжатых данных. */
	void readCompressedChunk();
};


/** Базовый класс для CompressedInputStream; содержит DecompressingStreamBuf
  */
class DecompressingIOS : public virtual std::ios
{
public:
	DecompressingIOS(std::istream & istr);
	DecompressingStreamBuf * rdbuf();

protected:
	DecompressingStreamBuf buf;
};


/** Разжимает данные, сжатые с помощью алгоритма QuickLZ.
  */
class CompressedInputStream : public DecompressingIOS, public std::istream
{
public:
	CompressedInputStream(std::istream & istr);
	int close();

	/** прочитать целиком один сжатый блок данных
      */
	void getChunk(std::vector<char> & res);
};


}


#endif
