#include <DB/IO/WriteBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>

#include <limits>
#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

/// Примечание: выделяется дополнительная страница, которая содежрит те данные, которые
/// не влезают в основной буфер.
WriteBufferAIO::WriteBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, mode_t mode_,
		char * existing_memory_)
		: WriteBufferFromFileBase(buffer_size_ + DEFAULT_AIO_FILE_BLOCK_SIZE, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
		flush_buffer(BufferWithOwnMemory<WriteBuffer>(this->memory.size(), nullptr, DEFAULT_AIO_FILE_BLOCK_SIZE)),
		filename(filename_)
{
	/// Исправить информацию о размере буферов, чтобы дополнительные страницы не касались базового класса BufferBase.
	this->buffer().resize(this->buffer().size() - DEFAULT_AIO_FILE_BLOCK_SIZE);
	this->internalBuffer().resize(this->internalBuffer().size() - DEFAULT_AIO_FILE_BLOCK_SIZE);
	flush_buffer.buffer().resize(this->buffer().size() - DEFAULT_AIO_FILE_BLOCK_SIZE);
	flush_buffer.internalBuffer().resize(this->internalBuffer().size() - DEFAULT_AIO_FILE_BLOCK_SIZE);

	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = (flags_ == -1) ? (O_RDWR | O_TRUNC | O_CREAT) : flags_;
	open_flags |= O_DIRECT;

	fd = ::open(filename.c_str(), open_flags, mode_);
	if (fd == -1)
	{
		auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
		throwFromErrno("Cannot open file " + filename, error_code);
	}

	ProfileEvents::increment(ProfileEvents::FileOpen);

	::memset(&request, 0, sizeof(request));
}

WriteBufferAIO::~WriteBufferAIO()
{
	if (!aio_failed)
	{
		try
		{
			flush();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

	if (fd != -1)
		::close(fd);
}

off_t WriteBufferAIO::getPositionInFile()
{
	return seek(0, SEEK_CUR);
}

void WriteBufferAIO::truncate(off_t length)
{
	flush();

	int res = ::ftruncate(fd, length);
	if (res == -1)
		throwFromErrno("Cannot truncate file " + filename, ErrorCodes::CANNOT_TRUNCATE_FILE);
}

void WriteBufferAIO::sync()
{
	flush();

	/// Попросим ОС сбросить данные на диск.
	int res = ::fsync(fd);
	if (res == -1)
		throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
}

void WriteBufferAIO::nextImpl()
{
	if (!offset())
		return;

	if (waitForAIOCompletion())
		finalize();

	/// Создать запрос на асинхронную запись.
	prepare();

	request.aio_lio_opcode = IOCB_CMD_PWRITE;
	request.aio_fildes = fd;
	request.aio_buf = reinterpret_cast<UInt64>(buffer_begin);
	request.aio_nbytes = region_aligned_size;
	request.aio_offset = region_aligned_begin;

	/// Отправить запрос.
	while (io_submit(aio_context.ctx, request_ptrs.size(), &request_ptrs[0]) < 0)
	{
		if (errno != EINTR)
		{
			aio_failed =  true;
			throw Exception("Cannot submit request for asynchronous IO on file " + filename, ErrorCodes::AIO_SUBMIT_ERROR);
		}
	}

	is_pending_write = true;
}

off_t WriteBufferAIO::doSeek(off_t off, int whence)
{
	flush();

	if (whence == SEEK_SET)
	{
		if (off < 0)
			throw Exception("SEEK_SET underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		pos_in_file = off;
	}
	else if (whence == SEEK_CUR)
	{
		if (off >= 0)
		{
			if (off > (std::numeric_limits<off_t>::max() - pos_in_file))
				throw Exception("SEEK_CUR overflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
		else if (off < -pos_in_file)
			throw Exception("SEEK_CUR underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		pos_in_file += off;
	}
	else
		throw Exception("WriteBufferAIO::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

	if (pos_in_file > max_pos_in_file)
		max_pos_in_file = pos_in_file;

	return pos_in_file;
}

void WriteBufferAIO::flush()
{
	next();
	if (waitForAIOCompletion())
		finalize();
}

bool WriteBufferAIO::waitForAIOCompletion()
{
	if (!is_pending_write)
		return false;

	while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
	{
		if (errno != EINTR)
		{
			aio_failed = true;
			throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
		}
	}

	is_pending_write = false;
	bytes_written = events[0].res;

	ProfileEvents::increment(ProfileEvents::WriteBufferAIOWrite);
	ProfileEvents::increment(ProfileEvents::WriteBufferAIOWriteBytes, bytes_written);

	return true;
}

void WriteBufferAIO::prepare()
{
	/// Менять местами основной и дублирующий буферы.
	buffer().swap(flush_buffer.buffer());
	std::swap(position(), flush_buffer.position());

	truncation_count = 0;

	/*
		Страница на диске или в памяти

		начальный адрес (начальная позиция в случае диска) кратен DEFAULT_AIO_FILE_BLOCK_SIZE
		:
		:
		+---------------+
		|               |
		|               |
		|               |
		|               |
		|               |
		|               |
		+---------------+
		<--------------->
		        :
		        :
		DEFAULT_AIO_FILE_BLOCK_SIZE

	*/

	/*
		Представление данных на диске

		XXX : данные, которые хотим записать
		ZZZ : данные, которые уже на диске или нули, если отсутствуют данные

		region_aligned_begin                                           region_aligned_end
		:   region_begin                                          region_end            :
		:   :                                                              :            :
		:   :                                                              :            :
		+---:-----------+---------------+---------------+---------------+--:------------+
		|   :           |               |               |               |  :            |
		|   +-----------+---------------+---------------+---------------+--+            |
		|ZZZ|XXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XX|ZZZZZZZZZZZZ|
		|ZZZ|XXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XX|ZZZZZZZZZZZZ|
		|   +-----------+---------------+---------------+---------------+--+            |
		|               |               |               |               |               |
		+---------------+---------------+---------------+---------------+---------------+

		<--><--------------------------------------------------------------><----------->
		 :                                    :                                   :
		 :                                    :                                   :
		region_left_padding             region_size                  region_right_padding

		<------------------------------------------------------------------------------->
		                                      :
		                                      :
		                              region_aligned_size
	*/

	/// Регион диска, в который хотим записать данные.
	const off_t region_begin = pos_in_file;

	if ((flush_buffer.offset() > std::numeric_limits<off_t>::max()) ||
		(pos_in_file > (std::numeric_limits<off_t>::max() - static_cast<off_t>(flush_buffer.offset()))))
		throw Exception("An overflow occurred during file operation", ErrorCodes::LOGICAL_ERROR);

	const off_t region_end = pos_in_file + flush_buffer.offset();
	const size_t region_size = region_end - region_begin;

	/// Выровненный регион диска, в который хотим записать данные.
	const size_t region_left_padding = region_begin % DEFAULT_AIO_FILE_BLOCK_SIZE;
	const size_t region_right_padding = (DEFAULT_AIO_FILE_BLOCK_SIZE - (region_end % DEFAULT_AIO_FILE_BLOCK_SIZE)) % DEFAULT_AIO_FILE_BLOCK_SIZE;

	region_aligned_begin = region_begin - region_left_padding;

	if (region_end > (std::numeric_limits<off_t>::max() - static_cast<off_t>(region_right_padding)))
		throw Exception("An overflow occurred during file operation", ErrorCodes::LOGICAL_ERROR);

	const off_t region_aligned_end = region_end + region_right_padding;
	region_aligned_size = region_aligned_end - region_aligned_begin;

	bytes_to_write = region_aligned_size;

	/*
		Представление данных в буфере до обработки

		XXX : данные, которые хотим записать

		buffer_begin                                         buffer_end
		:                                                             :
		:                                                             :
		+---------------+---------------+---------------+-------------:-+
		|               |               |               |             : |
		+---------------+---------------+---------------+-------------+ |
		|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXX| |
		|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXX| |
		+---------------+---------------+---------------+-------------+ |
		|               |               |               |               |
		+---------------+---------------+---------------+---------------+

		<------------------------------------------------------------->
		                                :
		                                :
		                            buffer_size
	*/

	/// Буфер данных, которые хотим записать на диск.
	buffer_begin = flush_buffer.buffer().begin();
	Position buffer_end = buffer_begin + region_size;
	size_t buffer_size = buffer_end - buffer_begin;

	/// Обработать буфер, чтобы он отражал структуру региона диска.

	/*
		Представление данных в буфере после обработки

		XXX : данные, которые хотим записать
		ZZZ : данные из диска или нули, если отсутствуют данные

		buffer_begin                                              buffer_end   дополнительная страница
		:                                                                  :       :
		:                                                                  :       :
		+---:-----------+---------------+---------------+---------------+--:------------+
		|               |               |               |               |  :            |
		|   +-----------+---------------+---------------+---------------+--+            |
		|ZZZ|XXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XX|ZZZZZZZZZZZZ|
		|ZZZ|XXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XXXXXXXXXXXXXXX|XX|ZZZZZZZZZZZZ|
		|   +-----------+---------------+---------------+---------------+--+            |
		|               |               |               |               |               |
		+---------------+---------------+---------------+---------------+---------------+

		<--><--------------------------------------------------------------><----------->
		 :                                  :                                     :
		 :                                  :                                     :
		region_left_padding             region_size                  region_right_padding

		<------------------------------------------------------------------------------->
		                                    :
		                                    :
		                             region_aligned_size
	*/

	if ((region_left_padding > 0) || (region_right_padding > 0))
	{
		char memory_page[DEFAULT_AIO_FILE_BLOCK_SIZE] __attribute__ ((aligned (DEFAULT_AIO_FILE_BLOCK_SIZE)));

		if (region_left_padding > 0)
		{
			/// Сдвинуть данные буфера вправо. Дополнить начало буфера данными из диска.
			buffer_size += region_left_padding;
			buffer_end = buffer_begin + buffer_size;

			::memmove(buffer_begin + region_left_padding, buffer_begin, buffer_size - region_left_padding);

			ssize_t read_count = ::pread(fd, memory_page, DEFAULT_AIO_FILE_BLOCK_SIZE, region_aligned_begin);
			if (read_count < 0)
				throw Exception("Read error", ErrorCodes::AIO_READ_ERROR);

			size_t to_copy = std::min(static_cast<size_t>(read_count), region_left_padding);
			::memcpy(buffer_begin, memory_page, to_copy);
			::memset(buffer_begin + to_copy, 0, region_left_padding - to_copy);
		}

		if (region_right_padding > 0)
		{
			/// Дополнить конец буфера данными из диска.
			ssize_t read_count = ::pread(fd, memory_page, DEFAULT_AIO_FILE_BLOCK_SIZE, region_aligned_end - DEFAULT_AIO_FILE_BLOCK_SIZE);
			if (read_count < 0)
				throw Exception("Read error", ErrorCodes::AIO_READ_ERROR);

			off_t offset = DEFAULT_AIO_FILE_BLOCK_SIZE - region_right_padding;
			if (read_count > offset)
			{
				::memcpy(buffer_end, memory_page + offset, read_count - offset);
				truncation_count = DEFAULT_AIO_FILE_BLOCK_SIZE - read_count;
			}
			else
				truncation_count = region_right_padding;
		}
	}
}

void WriteBufferAIO::finalize()
{
	if (bytes_written < bytes_to_write)
		throw Exception("Asynchronous write error on file " + filename, ErrorCodes::AIO_WRITE_ERROR);

	bytes_written -= truncation_count;

	off_t pos_offset = bytes_written - (pos_in_file - request.aio_offset);
	if (pos_in_file > (std::numeric_limits<off_t>::max() - pos_offset))
		throw Exception("An overflow occurred during file operation", ErrorCodes::LOGICAL_ERROR);
	pos_in_file += pos_offset;

	if (pos_in_file > max_pos_in_file)
		max_pos_in_file = pos_in_file;

	if (truncation_count > 0)
	{
		/// Укоротить файл, чтобы удалить из него излишние нули.
		int res = ::ftruncate(fd, max_pos_in_file);
		if (res == -1)
			throwFromErrno("Cannot truncate file " + filename, ErrorCodes::CANNOT_TRUNCATE_FILE);
	}
}

}
