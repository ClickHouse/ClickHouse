#pragma once

#include <DB/IO/WriteBufferFromFileBase.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <statdaemons/AIO.h>

#include <string>
#include <unistd.h>
#include <fcntl.h>

namespace DB
{

/** Класс для асинхронной записи данных.
  * Все размеры и смещения должны быть кратны DEFAULT_AIO_FILE_BLOCK_SIZE байтам.
  */
class WriteBufferAIO : public WriteBufferFromFileBase
{
public:
	WriteBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~WriteBufferAIO() override;

	WriteBufferAIO(const WriteBufferAIO &) = delete;
	WriteBufferAIO & operator=(const WriteBufferAIO &) = delete;

	off_t seek(off_t off, int whence = SEEK_SET) override;
	off_t getPositionInFile() override;
	void truncate(off_t length = 0) override;
	void sync() override;
	std::string getFileName() const noexcept override { return filename; }
	int getFD() const noexcept override { return fd; }

private:
	/// Если в буфере ещё остались данные - запишем их.
	void flush();
	///
	void nextImpl();
	/// Ждать окончания текущей асинхронной задачи.
	void waitForAIOCompletion();
	/// Менять местами основной и дублирующий буферы.
	void swapBuffers() noexcept;

private:
	/// Буфер для асинхронных операций записи данных.
	BufferWithOwnMemory<WriteBuffer> flush_buffer;

	iocb request;
	std::vector<iocb *> request_ptrs{&request};
	std::vector<io_event> events{1};

	AIOContext aio_context{1};

	const std::string filename;

	off_t pos_in_file = 0;
	int fd = -1;

	/// Асинхронная операция записи ещё не завершилась.
	bool is_pending_write = false;
	/// Было получено исключение.
	bool got_exception = false;
};

}
