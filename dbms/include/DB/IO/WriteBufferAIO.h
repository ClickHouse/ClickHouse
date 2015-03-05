#pragma once

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

namespace DB
{

using WriteBufferWithOwnMemory = BufferWithOwnMemory<WriteBuffer>;

class WriteBufferAIO : public WriteBufferWithOwnMemory
{
public:
	WriteBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~WriteBufferAIO() override;

	WriteBufferAIO(const WriteBufferAIO &) = delete;
	WriteBufferAIO & operator=(const WriteBufferAIO &) = delete;

	void sync();
	std::string getFileName() const { return filename; }
	int getFD() const { return fd; }

private:
	void swapBuffers();
	void waitForCompletion();
	void nextImpl() override;

private:
	static const size_t BLOCK_SIZE = 512;

private:
	WriteBufferWithOwnMemory flush_buffer; // buffer asynchronously flushed to disk
	const std::string filename; // name of the file to which we flush data.
	AIOContext aio_context;
	iocb cb;
	std::vector<iocb *> request_ptrs;
	std::vector<io_event> events;
	int fd = -1; // file descriptor
	bool is_pending_write = false;
};

}
