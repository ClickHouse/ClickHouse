#include <algorithm>

#include <DB/Common/CompressedOutputStream.h>


namespace DB
{


CompressingStreamBuf::CompressingStreamBuf(std::ostream & ostr)
	: Poco::BufferedStreamBuf(DBMS_COMPRESSING_STREAM_BUFFER_SIZE, std::ios::out),
	p_ostr(&ostr),
	compressed_buffer(DBMS_COMPRESSING_STREAM_BUFFER_SIZE + QUICKLZ_ADDITIONAL_SPACE),
	scratch(QLZ_SCRATCH_COMPRESS)
{
}


CompressingStreamBuf::~CompressingStreamBuf()
{
	close();
}


int CompressingStreamBuf::close()
{
	sync();
	return 0;
}


int CompressingStreamBuf::writeToDevice(const char * buffer, std::streamsize length)
{
	if (length == 0 || !p_ostr)
		return 0;

	size_t compressed_size = qlz_compress(
		buffer,
		&compressed_buffer[0],
		length,
		&scratch[0]);

	p_ostr->write(&compressed_buffer[0], compressed_size);
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
