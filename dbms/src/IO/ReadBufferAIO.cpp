#include <DB/IO/ReadBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Defines.h>

#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

/// Выделяем дополнительную страницу. Содежрит те данные, которые
/// не влезают в основной буфер.
ReadBufferAIO::ReadBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_,
	char * existing_memory_)
	: ReadBufferFromFileBase(buffer_size_ + DEFAULT_AIO_FILE_BLOCK_SIZE, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
	fill_buffer(BufferWithOwnMemory<ReadBuffer>(buffer_size_ + DEFAULT_AIO_FILE_BLOCK_SIZE, nullptr, DEFAULT_AIO_FILE_BLOCK_SIZE)),
	filename(filename_)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = (flags_ == -1) ? O_RDONLY : flags_;
	open_flags |= O_DIRECT;

	fd = ::open(filename.c_str(), open_flags);
	if (fd == -1)
	{
		got_exception = true;
		auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
		throwFromErrno("Cannot open file " + filename, error_code);
	}

	::memset(&request, 0, sizeof(request));
}

ReadBufferAIO::~ReadBufferAIO()
{
	try
	{
		(void) waitForAIOCompletion();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	if (fd != -1)
		::close(fd);
}

void ReadBufferAIO::setMaxBytes(size_t max_bytes_read_)
{
	if (is_started)
	{
		got_exception = true;
		throw Exception("Illegal attempt to set the maximum number of bytes to read from file " + filename, ErrorCodes::LOGICAL_ERROR);
	}
	max_bytes_read = max_bytes_read_;
}

off_t ReadBufferAIO::getPositionInFile()
{
	return seek(0, SEEK_CUR);
}

off_t ReadBufferAIO::doSeek(off_t off, int whence)
{
	off_t new_pos;

	if (whence == SEEK_SET)
	{
		if (off < 0)
		{
			got_exception = true;
			throw Exception("SEEK_SET underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
		new_pos = off;
	}
	else if (whence == SEEK_CUR)
	{
		if (off >= 0)
		{
			if (off > (std::numeric_limits<off_t>::max() - getPositionInFileRelaxed()))
			{
				got_exception = true;
				throw Exception("SEEK_CUR overflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
			}
		}
		else if (off < -getPositionInFileRelaxed())
		{
			got_exception = true;
			throw Exception("SEEK_CUR underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
		new_pos = getPositionInFileRelaxed() + off;
	}
	else
	{
		got_exception = true;
		throw Exception("ReadBufferAIO::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	if (new_pos != getPositionInFileRelaxed())
	{
		off_t working_buffer_begin_pos = pos_in_file - static_cast<off_t>(working_buffer.size());
		if (hasPendingData() && (new_pos >= working_buffer_begin_pos) && (new_pos <= pos_in_file))
		{
			/// Свдинулись, но остались в пределах буфера.
			pos = working_buffer.begin() + (new_pos - working_buffer_begin_pos);
		}
		else
		{
			pos = working_buffer.end();
			pos_in_file = new_pos;
		}

		/// Сдвинулись, значит не можем использовать результат последнего асинхронного запроса.
		skipPendingAIO();
	}

	return new_pos;
}

off_t ReadBufferAIO::getPositionInFileRelaxed() const noexcept
{
	return pos_in_file - (working_buffer.end() - pos);
}

bool ReadBufferAIO::nextImpl()
{
	/// Если конец файла уже был достигнут при вызове этой функции,
	/// то текущий вызов ошибочен.
	if (is_eof)
		return false;

	if (!is_started)
	{
		synchronousRead();
		is_started = true;
	}
	else
		sync();

	/// Если конец файла только что достигнут, больше ничего не делаем.
	if (is_eof)
		return true;

	/// Создать асинхронный запрос.
	initRequest();

	request.aio_lio_opcode = IOCB_CMD_PREAD;
	request.aio_fildes = fd;
	request.aio_buf = reinterpret_cast<UInt64>(buffer_begin);
	request.aio_nbytes = region_aligned_size;
	request.aio_offset = region_aligned_begin;

	/// Отправить запрос.
	while (io_submit(aio_context.ctx, request_ptrs.size(), &request_ptrs[0]) < 0)
	{
		if (errno != EINTR)
		{
			got_exception = true;
			throw Exception("Cannot submit request for asynchronous IO on file " + filename, ErrorCodes::AIO_SUBMIT_ERROR);
		}
	}

	is_pending_read = true;
	return true;
}

void ReadBufferAIO::synchronousRead()
{
	initRequest();
	bytes_read = ::pread(fd, buffer_begin, region_aligned_size, region_aligned_begin);
	publishReceivedData();
	swapBuffers();
}

void ReadBufferAIO::sync()
{
	if (!waitForAIOCompletion())
		return;
	publishReceivedData();
	swapBuffers();
}

void ReadBufferAIO::skipPendingAIO()
{
	if (!waitForAIOCompletion())
		return;

	is_started = false;

	/// Несмотря на то, что не станем использовать результат последнего запроса,
	/// убедимся, что запрос правильно выполнен.

	bytes_read = events[0].res;

	size_t region_left_padding = pos_in_file % DEFAULT_AIO_FILE_BLOCK_SIZE;

	if ((bytes_read < 0) || (static_cast<size_t>(bytes_read) < region_left_padding))
	{
		got_exception = true;
		throw Exception("Asynchronous read error on file " + filename, ErrorCodes::AIO_READ_ERROR);
	}
}

bool ReadBufferAIO::waitForAIOCompletion()
{
	if (is_eof || !is_pending_read)
		return false;

	while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
	{
		if (errno != EINTR)
		{
			got_exception = true;
			throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
		}
	}

	is_pending_read = false;
	bytes_read = events[0].res;

	return true;
}

void ReadBufferAIO::swapBuffers() noexcept
{
	internalBuffer().swap(fill_buffer.internalBuffer());
	buffer().swap(fill_buffer.buffer());
	std::swap(position(), fill_buffer.position());
}

void ReadBufferAIO::initRequest()
{
	/// Количество запрашиваемых байтов.
	requested_byte_count = std::min(fill_buffer.internalBuffer().size() - DEFAULT_AIO_FILE_BLOCK_SIZE, max_bytes_read);

	/// Регион диска, из которого хотим читать данные.
	const off_t region_begin = pos_in_file;
	const off_t region_end = pos_in_file + requested_byte_count;

	/// Выровненный регион диска, из которого хотим читать данные.
	const size_t region_left_padding = region_begin % DEFAULT_AIO_FILE_BLOCK_SIZE;
	const size_t region_right_padding = (DEFAULT_AIO_FILE_BLOCK_SIZE - (region_end % DEFAULT_AIO_FILE_BLOCK_SIZE)) % DEFAULT_AIO_FILE_BLOCK_SIZE;

	region_aligned_begin = region_begin - region_left_padding;
	const off_t region_aligned_end = region_end + region_right_padding;
	region_aligned_size = region_aligned_end - region_aligned_begin;

	/// Буфер, в который запишем данные из диска.
	buffer_begin = fill_buffer.internalBuffer().begin();
}

void ReadBufferAIO::publishReceivedData()
{
	size_t region_left_padding = pos_in_file % DEFAULT_AIO_FILE_BLOCK_SIZE;

	if ((bytes_read < 0) || (static_cast<size_t>(bytes_read) < region_left_padding))
	{
		got_exception = true;
		throw Exception("Asynchronous read error on file " + filename, ErrorCodes::AIO_READ_ERROR);
	}

	/// Игнорируем излишние байты слева.
	bytes_read -= region_left_padding;

	/// Игнорируем излишние байты справа.
	bytes_read = std::min(bytes_read, static_cast<off_t>(requested_byte_count));

	if (pos_in_file > (std::numeric_limits<off_t>::max() - bytes_read))
	{
		got_exception = true;
		throw Exception("File position overflowed", ErrorCodes::LOGICAL_ERROR);
	}

	::memmove(buffer_begin, buffer_begin + region_left_padding, bytes_read);

	if (bytes_read > 0)
		fill_buffer.buffer().resize(bytes_read);
	if (static_cast<size_t>(bytes_read) < requested_byte_count)
		is_eof = true;

	pos_in_file += bytes_read;
	total_bytes_read += bytes_read;

	if (total_bytes_read == max_bytes_read)
		is_eof = true;
}

}
