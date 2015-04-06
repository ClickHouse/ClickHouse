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
	off_t getPositionInFile() override { return pos_in_file - (working_buffer.end() - pos); }
	std::string getFileName() const noexcept override { return filename; }
	int getFD() const noexcept override { return fd; }

private:
	///
	bool nextImpl() override;
	///
	off_t doSeek(off_t off, int whence) override;
	/// Синхронно читать данные.
	void synchronousRead();
	/// Получить данные от асинхронного запроса.
	void receive();
	/// Игнорировать данные от асинхронного запроса.
	void skip();
	/// Ждать окончания текущей асинхронной задачи.
	bool waitForAIOCompletion();
	/// Подготовить асинхронный запрос.
	void prepare();
	/// Подготовить к чтению дублирующий буфер содержащий данные от
	/// последнего асинхронного запроса.
	void publish();

private:
	/// Буфер для асинхронных операций чтения данных.
	BufferWithOwnMemory<ReadBuffer> fill_buffer;

	/// Описание асинхронного запроса на чтение.
	iocb request;
	std::vector<iocb *> request_ptrs{&request};
	std::vector<io_event> events{1};

	AIOContext aio_context{1};

	const std::string filename;

	ssize_t bytes_read = 0;
	size_t max_bytes_read = std::numeric_limits<size_t>::max();
	size_t total_bytes_read = 0;
	/// Количество запрашиваемых байтов.
	size_t requested_byte_count = 0;
	off_t region_aligned_begin = 0;
	/// Текущая позиция в файле.
	off_t pos_in_file = 0;
	/// Файловый дескриптор для чтения.
	int fd = -1;

	Position buffer_begin = nullptr;
	size_t region_aligned_size = 0;

	/// Асинхронная операция чтения ещё не завершилась.
	bool is_pending_read = false;
	/// Конец файла достигнут.
	bool is_eof = false;
	/// Был отправлен хоть один запрос на асинхронную операцию чтения.
	bool is_started = false;
	/// Асинхронная операция завершилась неудачно?
	bool aio_failed = false;
};

}
