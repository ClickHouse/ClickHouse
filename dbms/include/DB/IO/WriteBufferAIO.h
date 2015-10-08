#pragma once

#include <DB/IO/WriteBufferFromFileBase.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Core/Defines.h>
#include <DB/Common/AIO.h>

#include <string>
#include <unistd.h>
#include <fcntl.h>

namespace DB
{

/** Класс для асинхронной записи данных.
  */
class WriteBufferAIO : public WriteBufferFromFileBase
{
public:
	WriteBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
		char * existing_memory_ = nullptr);
	~WriteBufferAIO() override;

	WriteBufferAIO(const WriteBufferAIO &) = delete;
	WriteBufferAIO & operator=(const WriteBufferAIO &) = delete;

	off_t getPositionInFile() override;
	void sync() override;
	std::string getFileName() const override { return filename; }
	int getFD() const override { return fd; }

private:
	///
	void nextImpl() override;
	///
	off_t doSeek(off_t off, int whence) override;
	///
	void doTruncate(off_t length) override;
	/// Если в буфере ещё остались данные - запишем их.
	void flush();
	/// Ждать окончания текущей асинхронной задачи.
	bool waitForAIOCompletion();
	/// Подготовить асинхронный запрос.
	void prepare();
	///
	void finalize();

private:
	/// Буфер для асинхронных операций записи данных.
	BufferWithOwnMemory<WriteBuffer> flush_buffer;

	/// Описание асинхронного запроса на запись.
	iocb request = { 0 };
	std::vector<iocb *> request_ptrs{&request};
	std::vector<io_event> events{1};

	AIOContext aio_context{1};

	const std::string filename;

	/// Количество байтов, которые будут записаны на диск.
	off_t bytes_to_write = 0;
	/// Количество записанных байт при последнем запросе.
	off_t bytes_written = 0;
	/// Количество нулевых байтов, которые надо отрезать c конца файла
	/// после завершения операции записи данных.
	off_t truncation_count = 0;

	/// Текущая позиция в файле.
	off_t pos_in_file = 0;
	/// Максимальная достигнутая позиция в файле.
	off_t max_pos_in_file = 0;

	/// Начальная позиция выровненного региона диска, в который записываются данные.
	off_t region_aligned_begin = 0;
	/// Размер выровненного региона диска.
	size_t region_aligned_size = 0;

	/// Файловый дескриптор для записи.
	int fd = -1;

	/// Буфер данных, которые хотим записать на диск.
	Position buffer_begin = nullptr;

	/// Асинхронная операция записи ещё не завершилась?
	bool is_pending_write = false;
	/// Асинхронная операция завершилась неудачно?
	bool aio_failed = false;
};

}
