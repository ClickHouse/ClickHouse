#pragma once

#include <DB/IO/IBufferAIO.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

#include <unistd.h>
#include <fcntl.h>

namespace DB
{

/** Класс для асинхронной записи данных.
  * Все размеры и смещения должны быть кратны 512 байтам.
  */
class WriteBufferAIO : public IBufferAIO, public BufferWithOwnMemory<WriteBuffer>
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
	/// Буфер для асинхронных операций записи данных.
	BufferWithOwnMemory<WriteBuffer> flush_buffer;

	iocb request;
	std::vector<iocb *> request_ptrs;
	std::vector<io_event> events;

	AIOContext aio_context;

	const std::string filename;

	size_t total_bytes_written = 0;
	int fd = -1;

	/// Асинхронная операция записи ещё не завершилась.
	bool is_pending_write = false;
	/// Было получено исключение.
	bool got_exception = false;
};

}
