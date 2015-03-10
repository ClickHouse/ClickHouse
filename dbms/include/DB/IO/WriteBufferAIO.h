#pragma once

#include <DB/IO/IBufferAIO.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

#include <unistd.h>
#include <fcntl.h>

namespace DB
{

using WriteBufferWithOwnMemory = BufferWithOwnMemory<WriteBuffer>;

class WriteBufferAIO : public IBufferAIO, public WriteBufferWithOwnMemory
{
public:
	WriteBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~WriteBufferAIO() override;

	WriteBufferAIO(const WriteBufferAIO &) = delete;
	WriteBufferAIO & operator=(const WriteBufferAIO &) = delete;

	off_t seek(off_t off, int whence = SEEK_SET);
	void truncate(off_t length = 0);
	void sync();
	std::string getFileName() const noexcept override { return filename; }
	int getFD() const noexcept override { return fd; }

private:
	void nextImpl() override;
	void waitForCompletion() override;
	void swapBuffers() noexcept override;

private:
	WriteBufferWithOwnMemory flush_buffer; // buffer asynchronously flushed to disk
	const std::string filename; // name of the file to which we flush data.

	AIOContext aio_context;
	iocb cb;
	std::vector<iocb *> request_ptrs;
	std::vector<io_event> events;

	int fd = -1; // file descriptor
	size_t total_bytes_written = 0;

	bool is_pending_write = false;
	bool got_exception = false;
};

}
