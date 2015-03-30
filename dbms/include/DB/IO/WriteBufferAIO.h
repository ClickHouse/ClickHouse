#pragma once

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Core/Defines.h>
#include <statdaemons/AIO.h>

#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/uio.h>

namespace DB
{

/** Класс для асинхронной записи данных.
  * Размер буфера должен составить не менее двух страниц.
  */
class WriteBufferAIO : public BufferWithOwnMemory<WriteBuffer>
{
public:
	WriteBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~WriteBufferAIO() override;

	WriteBufferAIO(const WriteBufferAIO &) = delete;
	WriteBufferAIO & operator=(const WriteBufferAIO &) = delete;

	off_t seek(off_t off, int whence = SEEK_SET);
	off_t getPositionInFile();
	void truncate(off_t length = 0);
	void sync();
	std::string getFileName() const noexcept { return filename; }
	int getFD() const noexcept { return fd; }

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

	/// Описание асинхронного запроса на запись.
	iocb request;
	std::vector<iocb *> request_ptrs{&request};
	std::vector<io_event> events{1};

	AIOContext aio_context{1};

	iovec iov[3];

	/// Дополнительный буфер размером со страницу. Содежрит те данные, которые
	/// не влезают в основной буфер.
	Memory memory_page{DEFAULT_AIO_FILE_BLOCK_SIZE, DEFAULT_AIO_FILE_BLOCK_SIZE};

	const std::string filename;

	/// Количество байтов, которые будут записаны на диск.
	off_t bytes_to_write = 0;

	/// Количество нулевых байтов, которые надо отрезать c конца файла
	/// после завершения операции записи данных.
	off_t truncation_count = 0;

	/// Текущая позиция в файле.
	off_t pos_in_file = 0;
	/// Максимальная достигнутая позиция в файле.
	off_t max_pos_in_file = 0;

	/// Файловый дескриптор для записи.
	int fd = -1;
	/// Файловый дескриптор для чтения. Употребляется для невыровненных записей.
	int fd2 = -1;

	/// Асинхронная операция записи ещё не завершилась?
	bool is_pending_write = false;
	/// Было получено исключение?
	bool got_exception = false;
};

}
