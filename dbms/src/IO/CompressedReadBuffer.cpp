#include <DB/IO/CompressedReadBuffer.h>


namespace DB
{

bool CompressedReadBuffer::nextImpl()
{
	size_t size_decompressed;
	size_t size_compressed_without_checksum;
	size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
	if (!size_compressed)
		return false;

	memory.resize(size_decompressed);
	working_buffer = Buffer(&memory[0], &memory[size_decompressed]);

	decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

	return true;
}

size_t CompressedReadBuffer::readBig(char * to, size_t n)
{
	size_t bytes_read = 0;

	/// Если в буфере есть непрочитанные байты, то скопируем сколько надо в to.
	if (pos < working_buffer.end())
		bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

	/// Если надо ещё прочитать - будем, по возможности, разжимать сразу в to.
	while (bytes_read < n)
	{
		size_t size_decompressed;
		size_t size_compressed_without_checksum;

		if (!readCompressedData(size_decompressed, size_compressed_without_checksum))
			return bytes_read;

		/// Если разжатый блок помещается целиком туда, куда его надо скопировать.
		if (size_decompressed <= n - bytes_read)
		{
			decompress(to + bytes_read, size_decompressed, size_compressed_without_checksum);
			bytes_read += size_decompressed;
			bytes += size_decompressed;
		}
		else
		{
			bytes += offset();
			memory.resize(size_decompressed);
			working_buffer = Buffer(&memory[0], &memory[size_decompressed]);
			pos = working_buffer.begin();

			decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

			bytes_read += read(to + bytes_read, n - bytes_read);
			break;
		}
	}

	return bytes_read;
}

}
