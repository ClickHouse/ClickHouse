#pragma once

#include <DB/IO/IBufferAIO.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

#include <limits>

#include <unistd.h>
#include <fcntl.h>

namespace DB
{

using ReadBufferWithOwnMemory = BufferWithOwnMemory<ReadBuffer>;

class ReadBufferAIO : public IBufferAIO, public ReadBufferWithOwnMemory
{
public:
	ReadBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~ReadBufferAIO() override;

	ReadBufferAIO(const ReadBufferAIO &) = delete;
	ReadBufferAIO & operator=(const ReadBufferAIO &) = delete;

	void setMaxBytes(size_t max_bytes_read_);
	off_t seek(off_t off, int whence = SEEK_SET);
	size_t getPositionInFile() const noexcept { return pos_in_file - (working_buffer.end() - pos); }
	std::string getFileName() const noexcept { return filename; }
	int getFD() const noexcept { return fd; }

private:
	bool nextImpl() override;
	void waitForCompletion();
	void swapBuffers() noexcept;

private:
	ReadBufferWithOwnMemory fill_buffer; // buffer asynchronously read from disk
	const std::string filename;
	AIOContext aio_context;
	iocb cb;
	std::vector<iocb *> request_ptrs;
	std::vector<io_event> events;
	size_t max_bytes_read = std::numeric_limits<size_t>::max();
	size_t total_bytes_read = 0;
	int fd = -1; // file descriptor
	off_t pos_in_file = 0;
	bool is_pending_read = false;
	bool got_exception = false;
	bool is_eof = false;
	bool is_started = false;
};

}
