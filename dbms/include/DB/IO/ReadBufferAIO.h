#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

#include <string>
#include <limits>
#include <unistd.h>
#include <fcntl.h>

namespace DB
{

/** Класс для асинхронной чтения данных.
  * Все размеры и смещения должны быть кратны DEFAULT_AIO_FILE_BLOCK_SIZE байтам.
  */
class ReadBufferAIO : public BufferWithOwnMemory<ReadBuffer>
{
public:
	ReadBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~ReadBufferAIO() override;

	ReadBufferAIO(const ReadBufferAIO &) = delete;
	ReadBufferAIO & operator=(const ReadBufferAIO &) = delete;

	void setMaxBytes(size_t max_bytes_read_);
	off_t seek(off_t off, int whence = SEEK_SET);
	off_t getPositionInFile();
	std::string getFileName() const noexcept { return filename; }
	int getFD() const noexcept { return fd; }

private:
	off_t getPositionInFileRelaxed() const noexcept;
	bool nextImpl();
	/// Ждать окончания текущей асинхронной задачи.
	void waitForCompletion();
	/// Менять местами основной и дублирующий буферы.
	void swapBuffers() noexcept;

private:
	/// Буфер для асинхронных операций чтения данных.
	BufferWithOwnMemory<ReadBuffer> fill_buffer;

	iocb request;
	std::vector<iocb *> request_ptrs;
	std::vector<io_event> events;

	AIOContext aio_context;

	const std::string filename;

	size_t max_bytes_read = std::numeric_limits<size_t>::max();
	size_t total_bytes_read = 0;
	off_t pos_in_file = 0;
	int fd = -1;

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
