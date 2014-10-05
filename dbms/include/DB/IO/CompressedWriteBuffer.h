#pragma once

#include <math.h>

#include <vector>

#include <city.h>
#include <quicklz/quicklz_level1.h>
#include <lz4/lz4.h>
#include <lz4/lz4hc.h>

#include <DB/Common/PODArray.h>
#include <DB/Core/Types.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
	WriteBuffer & out;
	CompressionMethod::Enum method;

	PODArray<char> compressed_buffer;
	qlz_state_compress * qlz_state;

	void nextImpl()
	{
		if (!offset())
			return;

		size_t uncompressed_size = offset();
		size_t compressed_size = 0;
		char * compressed_buffer_ptr = nullptr;

		/** Формат сжатого блока следующий:
		  *
		  * Первые 16 байт - чексумма от всех остальных байт блока. Сейчас используется только CityHash128.
		  * В дальнейшем можно предусмотреть другие чексуммы, хотя сделать их другого размера не получится.
		  *
		  * Следующий байт определяет алгоритм сжатия. Далее всё зависит от алгоритма.
		  *
		  * Первые 4 варианта совместимы с QuickLZ level 1.
		  * То есть, если значение первого байта < 4, для разжатия достаточно использовать функцию qlz_level1_decompress.
		  *
		  * 0x00 - несжатые данные, маленький блок. Далее один байт - размер сжатых данных, с учётом заголовка; один байт - размер несжатых данных.
		  * 0x01 - сжатые данные, QuickLZ level 1, маленький блок. Далее два байта аналогично.
		  * 0x02 - несжатые данные, большой блок. Далее 4 байта - размер сжатых данных, с учётом заголовка; 4 байта - размер несжатых данных.
		  * 0x03 - сжатые данные, QuickLZ level 1, большой блок. Далее 8 байт аналогично.
		  *
		  * 0x82 - LZ4 или LZ4HC (они имеют одинаковый формат).
		  *        Далее 4 байта - размер сжатых данных, с учётом заголовка; 4 байта - размер несжатых данных.
		  *
		  * NOTE: Почему 0x82?
		  * Изначально использовался только QuickLZ. Потом был добавлен LZ4.
		  * Старший бит выставлен, чтобы отличить от QuickLZ, а второй бит выставлен для совместимости,
		  *  чтобы работали функции qlz_size_compressed, qlz_size_decompressed.
		  * Хотя сейчас такая совместимость уже не актуальна.
		  *
		  * Все размеры - little endian.
		  */

		switch (method)
		{
			case CompressionMethod::QuickLZ:
			{
				compressed_buffer.resize(uncompressed_size + QUICKLZ_ADDITIONAL_SPACE);

				compressed_size = qlz_compress(
					working_buffer.begin(),
					&compressed_buffer[0],
					uncompressed_size,
					qlz_state);

				compressed_buffer[0] &= 3;
				compressed_buffer_ptr = &compressed_buffer[0];
				break;
			}
			case CompressionMethod::LZ4:
			case CompressionMethod::LZ4HC:
			{
				static constexpr size_t header_size = 1 + sizeof(UInt32) + sizeof(UInt32);

				compressed_buffer.resize(header_size + LZ4_COMPRESSBOUND(uncompressed_size));

				compressed_buffer[0] = 0x82;	/// Второй бит - для совместимости с QuickLZ - обозначает, что размеры записываются 4 байтами.

				if (method == CompressionMethod::LZ4)
					compressed_size = header_size + LZ4_compress(
						working_buffer.begin(),
						&compressed_buffer[header_size],
						uncompressed_size);
				else
					compressed_size = header_size + LZ4_compressHC(
						working_buffer.begin(),
						&compressed_buffer[header_size],
						uncompressed_size);

				UInt32 compressed_size_32 = compressed_size;
				UInt32 uncompressed_size_32 = uncompressed_size;

				memcpy(&compressed_buffer[1], reinterpret_cast<const char *>(&compressed_size_32), sizeof(compressed_size_32));
				memcpy(&compressed_buffer[5], reinterpret_cast<const char *>(&uncompressed_size_32), sizeof(uncompressed_size_32));

				compressed_buffer_ptr = &compressed_buffer[0];
				break;
			}
			default:
				throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		}

		uint128 checksum = CityHash128(compressed_buffer_ptr, compressed_size);
		out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));

		out.write(compressed_buffer_ptr, compressed_size);
	}

public:
	CompressedWriteBuffer(
		WriteBuffer & out_,
		CompressionMethod::Enum method_ = CompressionMethod::LZ4,
		size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), method(method_), qlz_state(new qlz_state_compress) {}

	/// Объём сжатых данных
	size_t getCompressedBytes()
	{
		nextIfAtEnd();
		return out.count();
	}

	/// Сколько несжатых байт было записано в буфер
	size_t getUncompressedBytes()
	{
		return count();
	}

	/// Сколько байт находится в буфере (ещё не сжато)
	size_t getRemainingBytes()
	{
		nextIfAtEnd();
		return offset();
	}

	~CompressedWriteBuffer()
	{
		try
		{
			next();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		delete qlz_state;
	}
};

}
