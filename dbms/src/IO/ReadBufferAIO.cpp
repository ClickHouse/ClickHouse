#include <DB/IO/ReadBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Defines.h>

#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

ReadBufferAIO::ReadBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_,
	char * existing_memory_)
	: ReadBufferFromFileBase(buffer_size_, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
	fill_buffer(BufferWithOwnMemory<ReadBuffer>(buffer_size_, nullptr, DEFAULT_AIO_FILE_BLOCK_SIZE)),
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
	if (!got_exception)
	{
		try
		{
			sync();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
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

off_t ReadBufferAIO::doSeek(off_t off, int whence)
{
	sync();

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
	}

	return new_pos;
}

off_t ReadBufferAIO::getPositionInFile()
{
	return seek(0, SEEK_CUR);
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

	sync();
	is_started = true;

	/// Если конец файла только что достигнут, больше ничего не делаем.
	if (is_eof)
		return true;

	/// Количество запрашиваемых байтов.
	requested_byte_count = std::min(fill_buffer.internalBuffer().size(), max_bytes_read);

	/// Регион диска, из которого хотим читать данные.
	const off_t region_begin = pos_in_file;
	const off_t region_end = pos_in_file + requested_byte_count;
	const size_t region_size = region_end - region_begin;

	/// Выровненный регион диска, из которого хотим читать данные.
	const size_t region_left_padding = region_begin % DEFAULT_AIO_FILE_BLOCK_SIZE;
	const size_t region_right_padding = (DEFAULT_AIO_FILE_BLOCK_SIZE - (region_end % DEFAULT_AIO_FILE_BLOCK_SIZE)) % DEFAULT_AIO_FILE_BLOCK_SIZE;

	const off_t region_aligned_begin = region_begin - region_left_padding;
	const off_t region_aligned_end = region_end + region_right_padding;
	const off_t region_aligned_size = region_aligned_end - region_aligned_begin;

	/// Буфер, в который запишем данные из диска.
	const Position buffer_begin = fill_buffer.internalBuffer().begin();
	buffer_capacity = this->memory.size();

	size_t excess_count = 0;

	if ((region_left_padding + region_size) > buffer_capacity)
	{
		excess_count = region_left_padding + region_size - buffer_capacity;
		::memset(&memory_page[0], 0, memory_page.size());
	}

	/// Создать запрос на асинхронное чтение.

	size_t i = 0;

	iov[i].iov_base = buffer_begin;
	iov[i].iov_len = (excess_count > 0) ? buffer_capacity : region_aligned_size;
	++i;

	if (excess_count > 0)
	{
		iov[i].iov_base = &memory_page[0];
		iov[i].iov_len = memory_page.size();
		++i;
	}

	request.aio_lio_opcode = IOCB_CMD_PREADV;
	request.aio_fildes = fd;
	request.aio_buf = reinterpret_cast<UInt64>(iov);
	request.aio_nbytes = i;
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

void ReadBufferAIO::sync()
{
	if (is_eof || !is_started || !is_pending_read)
		return;

	waitForAIOCompletion();
	swapBuffers();
}

void ReadBufferAIO::waitForAIOCompletion()
{
	if (!is_pending_read)
		return;

	while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
	{
		if (errno != EINTR)
		{
			got_exception = true;
			throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
		}
	}

	is_pending_read = false;
	off_t bytes_read = events[0].res;

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

	Position buffer_begin = fill_buffer.internalBuffer().begin();

	if ((region_left_padding + bytes_read) > buffer_capacity)
	{
		size_t excess_count = region_left_padding + bytes_read - buffer_capacity;
		::memmove(buffer_begin, buffer_begin + region_left_padding, buffer_capacity);
		::memcpy(buffer_begin + buffer_capacity - region_left_padding, &memory_page[0], excess_count);
	}
	else
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

void ReadBufferAIO::swapBuffers() noexcept
{
	internalBuffer().swap(fill_buffer.internalBuffer());
	buffer().swap(fill_buffer.buffer());
	std::swap(position(), fill_buffer.position());
}

}
