#pragma once

#include <memory>

#ifdef USE_QUICKLZ
	struct qlz_state_compress;
#endif

#include <DB/Common/PODArray.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/CompressedStream.h>


namespace DB
{

class CompressedWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
	WriteBuffer & out;
	CompressionMethod method;

	PODArray<char> compressed_buffer;

#ifdef USE_QUICKLZ
	std::unique_ptr<qlz_state_compress> qlz_state;
#else
	/// ABI compatibility for USE_QUICKLZ
	void * fixed_size_padding = nullptr;
	/// Отменяет warning unused-private-field.
	void * fixed_size_padding_used() const { return fixed_size_padding; }
#endif

	void nextImpl() override;

public:
	CompressedWriteBuffer(
		WriteBuffer & out_,
		CompressionMethod method_ = CompressionMethod::LZ4,
		size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

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

	~CompressedWriteBuffer() override;
};

}
