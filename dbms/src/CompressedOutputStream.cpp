#include <algorithm>

#include <DB/CompressedOutputStream.h>


namespace DB
{


CompressingStreamBuf::CompressingStreamBuf(std::ostream & ostr)
	: Poco::BufferedStreamBuf(DBMS_STREAM_BUFFER_SIZE, std::ios::out),
	pos_in_buffer(0),
	p_ostr(&ostr),
	uncompressed_buffer(DBMS_COMPRESSING_STREAM_BUFFER_SIZE),
	compressed_buffer(DBMS_COMPRESSING_STREAM_BUFFER_SIZE + QUICKLZ_ADDITIONAL_SPACE),
	scratch(QLZ_SCRATCH_COMPRESS)
{
}


CompressingStreamBuf::~CompressingStreamBuf()
{
	close();
}


void CompressingStreamBuf::writeCompressedChunk()
{
	size_t compressed_size = qlz_compress(
		&uncompressed_buffer[0],
		&compressed_buffer[0],
		pos_in_buffer,
		&scratch[0]);

	p_ostr->write(&compressed_buffer[0], compressed_size);
	pos_in_buffer = 0;
}


int CompressingStreamBuf::close()
{
	sync();

	if (pos_in_buffer != 0)
		writeCompressedChunk();
	
	return 0;
}


int CompressingStreamBuf::writeToDevice(const char * buffer, std::streamsize length)
{
	if (length == 0 || !p_ostr)
		return 0;

	size_t bytes_processed = 0;

	while (bytes_processed < static_cast<size_t>(length))
	{
		size_t bytes_to_copy = std::min(
			uncompressed_buffer.size() - pos_in_buffer,
			static_cast<size_t>(length) - bytes_processed);
		memcpy(&uncompressed_buffer[pos_in_buffer], buffer + bytes_processed, bytes_to_copy);
		pos_in_buffer += bytes_to_copy;
		bytes_processed += bytes_to_copy;

		if (pos_in_buffer == uncompressed_buffer.size())
			writeCompressedChunk();

		if (!p_ostr->good())
		{
			p_ostr = 0;
			return bytes_processed;
		}
	}

	return static_cast<int>(length);
}


CompressingIOS::CompressingIOS(std::ostream & ostr)
	: buf(ostr)
{
	poco_ios_init(&buf);
}


CompressingStreamBuf * CompressingIOS::rdbuf()
{
	return &buf;
}


CompressedOutputStream::CompressedOutputStream(std::ostream & ostr)
	: CompressingIOS(ostr),
	std::ostream(&buf)
{
}


int CompressedOutputStream::close()
{
	return buf.close();
}


}
