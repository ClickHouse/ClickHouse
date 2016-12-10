#pragma once

#include <DB/IO/CompressedReadBufferBase.h>
#include <DB/IO/ReadBufferFromFileBase.h>

#include <time.h>
#include <memory>


namespace DB
{


/// В отличие от CompressedReadBuffer, умеет делать seek.
class CompressedReadBufferFromFile : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
	/** В любой момент выполняется одно из двух:
	  * a) size_compressed = 0
	  * b)
	  *  - working_buffer содержит целиком один блок.
	  *  - file_in смотрит в конец этого блока.
	  *  - size_compressed содержит сжатый размер этого блока.
	  */
	std::unique_ptr<ReadBufferFromFileBase> p_file_in;
	ReadBufferFromFileBase & file_in;
	size_t size_compressed = 0;

	bool nextImpl() override;

public:
	CompressedReadBufferFromFile(
		const std::string & path, size_t estimated_size, size_t aio_threshold, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

	void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);

	size_t readBig(char * to, size_t n) override;

	void setProfileCallback(const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
	{
		file_in.setProfileCallback(profile_callback_, clock_type_);
	}
};

}
