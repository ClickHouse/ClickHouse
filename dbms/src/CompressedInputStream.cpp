#include <algorithm>

#include <DB/CompressedInputStream.h>


namespace DB
{


DecompressingStreamBuf::DecompressingStreamBuf(std::istream & istr)
	: Poco::BufferedStreamBuf(DBMS_STREAM_BUFFER_SIZE, std::ios::in),
	pos_in_buffer(0),
	p_istr(&istr),
	compressed_buffer(QUICKLZ_HEADER_SIZE),
	scratch(QLZ_SCRATCH_DECOMPRESS)
{
}


void DecompressingStreamBuf::readCompressedChunk()
{
	/// прочитаем заголовок
	p_istr->read(&compressed_buffer[0], QUICKLZ_HEADER_SIZE);

	if (!p_istr->good())
		return;

	size_t size_compressed = qlz_size_compressed(&compressed_buffer[0]);
	size_t size_decompressed = qlz_size_decompressed(&compressed_buffer[0]);

	compressed_buffer.resize(size_compressed);
	uncompressed_buffer.resize(size_decompressed);

	/// считаем остаток сжатого блока

	p_istr->read(&compressed_buffer[QUICKLZ_HEADER_SIZE], size_compressed - QUICKLZ_HEADER_SIZE);

	if (!p_istr->good())
		return;

	/// разжимаем блок

	qlz_decompress(&compressed_buffer[0], &uncompressed_buffer[0], &scratch[0]);
}


int DecompressingStreamBuf::readFromDevice(char * buffer, std::streamsize length)
{
	if (length == 0 || !p_istr)
		return 0;

	size_t bytes_processed = 0;

	while (bytes_processed < static_cast<size_t>(length))
	{
		if (pos_in_buffer == uncompressed_buffer.size())
		{
			readCompressedChunk();
			pos_in_buffer = 0;

			if (!p_istr->good())
			{
				p_istr = 0;
				return bytes_processed;
			}
		}

		size_t bytes_to_copy = std::min(
			uncompressed_buffer.size() - pos_in_buffer,
			static_cast<size_t>(length) - bytes_processed);
		
		memcpy(buffer + bytes_processed, &uncompressed_buffer[pos_in_buffer], bytes_to_copy);
		pos_in_buffer += bytes_to_copy;
		bytes_processed += bytes_to_copy;
	}

	return static_cast<int>(length);
}


DecompressingIOS::DecompressingIOS(std::istream & istr)
	: buf(istr)
{
	poco_ios_init(&buf);
}


DecompressingStreamBuf * DecompressingIOS::rdbuf()
{
	return &buf;
}


CompressedInputStream::CompressedInputStream(std::istream & istr)
	: DecompressingIOS(istr),
	std::istream(&buf)
{
}

}
