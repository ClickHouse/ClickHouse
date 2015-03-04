#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

namespace DB
{

using ReadBufferWithOwnMemory = BufferWithOwnMemory<ReadBuffer>;

class ReadBufferAIO : public ReadBufferWithOwnMemory
{
public:
	ReadBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~ReadBufferAIO() override;

	ReadBufferAIO(const ReadBufferAIO &) = delete;
	ReadBufferAIO & operator=(const ReadBufferAIO &) = delete;

	std::string getFileName() const;

private:
	void swapBuffers();
	void waitForCompletion();
	bool nextImpl() override;

private:
	static const size_t BLOCK_SIZE = 512;

private:
	ReadBufferWithOwnMemory fill_buffer; // buffer asynchronously read from disk
	const std::string filename;
	AIOContext aio_context;
	iocb cb;
	std::vector<iocb *> request_ptrs;
	std::vector<io_event> events;
	int fd = -1; // file descriptor
	bool is_pending_read = false;
};

}
