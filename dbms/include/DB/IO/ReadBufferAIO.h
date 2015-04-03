#pragma once

#include <DB/IO/ReadBufferFromFileBase.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Core/Defines.h>
#include <statdaemons/AIO.h>

#include <string>
#include <limits>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>

namespace DB
{

/** Класс для асинхронного чтения данных.
  */
class ReadBufferAIO : public ReadBufferFromFileBase
{
public:
	ReadBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1,
		char * existing_memory_ = nullptr);
	~ReadBufferAIO() override;

	ReadBufferAIO(const ReadBufferAIO &) = delete;
	ReadBufferAIO & operator=(const ReadBufferAIO &) = delete;

	void setMaxBytes(size_t max_bytes_read_);
	off_t getPositionInFile() override;
	std::string getFileName() const noexcept override { return filename; }
	int getFD() const noexcept override { return fd; }

private:
	off_t getPositionInFileRelaxed() const noexcept;
	off_t doSeek(off_t off, int whence) override;
	bool nextImpl() override;
	void sync() override;
	/// Ждать окончания текущей асинхронной задачи.
	void waitForAIOCompletion();
	/// Менять местами основной и дублирующий буферы.
	void swapBuffers() noexcept;

private:
	/// Буфер для асинхронных операций чтения данных.
	BufferWithOwnMemory<ReadBuffer> fill_buffer;

	iocb request;
	std::vector<iocb *> request_ptrs{&request};
	std::vector<io_event> events{1};

	AIOContext aio_context{1};

	iovec iov[2];

	/// Дополнительный буфер размером со страницу. Содежрит те данные, которые
	/// не влезают в основной буфер.
	Memory memory_page{DEFAULT_AIO_FILE_BLOCK_SIZE, DEFAULT_AIO_FILE_BLOCK_SIZE};

	const std::string filename;

	size_t max_bytes_read = std::numeric_limits<size_t>::max();
	size_t total_bytes_read = 0;
	size_t requested_byte_count = 0;
	off_t pos_in_file = 0;
	int fd = -1;

	size_t buffer_capacity = 0;

	/// Асинхронная операция чтения ещё не завершилась.
	bool is_pending_read = false;
	/// Было получено исключение.
	bool got_exception = false;
	/// Конец файла достигнут.
	bool is_eof = false;
	/// Был отправлен хоть один запрос на асинхронную операцию чтения.
	bool is_started = false;
};

}
