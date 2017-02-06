#pragma once

#include <DB/IO/ReadBufferFromFileBase.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/Core/Defines.h>
#include <DB/Common/AIO.h>
#include <DB/Common/CurrentMetrics.h>

#include <string>
#include <limits>
#include <unistd.h>
#include <fcntl.h>


namespace CurrentMetrics
{
	extern const Metric OpenFileForRead;
}

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
	off_t getPositionInFile() override { return first_unread_pos_in_file - (working_buffer.end() - pos); }
	std::string getFileName() const override { return filename; }
	int getFD() const override { return fd; }

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
	/// Подготовить запрос.
	void prepare();
	/// Подготовить к чтению дублирующий буфер содержащий данные от
	/// последнего запроса.
	void finalize();

private:
	/// Буфер для асинхронных операций чтения данных.
	BufferWithOwnMemory<ReadBuffer> fill_buffer;

	/// Описание асинхронного запроса на чтение.
	iocb request{};
	std::future<ssize_t> future_bytes_read;

	const std::string filename;

	/// Максимальное количество байтов, которое можно прочитать.
	size_t max_bytes_read = std::numeric_limits<size_t>::max();
	/// Количество запрашиваемых байтов.
	size_t requested_byte_count = 0;
	/// Количество прочитанных байт при последнем запросе.
	ssize_t bytes_read = 0;
	/// Итоговое количество прочитанных байтов.
	size_t total_bytes_read = 0;

	/// Позиция первого непрочитанного байта в файле.
	off_t first_unread_pos_in_file = 0;

	/// Начальная позиция выровненного региона диска, из которого читаются данные.
	off_t region_aligned_begin = 0;
	/// Левое смещение для выравнения региона диска.
	size_t region_left_padding = 0;
	/// Размер выровненного региона диска.
	size_t region_aligned_size = 0;

	/// Файловый дескриптор для чтения.
	int fd = -1;

	/// Буфер, в который пишутся полученные данные.
	Position buffer_begin = nullptr;

	/// Асинхронная операция чтения ещё не завершилась.
	bool is_pending_read = false;
	/// Конец файла достигнут.
	bool is_eof = false;
	/// Был отправлен хоть один запрос на чтение.
	bool is_started = false;
	/// Является ли операция асинхронной?
	bool is_aio = false;
	/// Асинхронная операция завершилась неудачно?
	bool aio_failed = false;

	CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};
};

}
