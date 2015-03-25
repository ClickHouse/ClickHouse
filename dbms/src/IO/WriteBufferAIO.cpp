#include <DB/IO/WriteBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>

#include <limits>
#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

WriteBufferAIO::WriteBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, mode_t mode_,
		char * existing_memory_)
		: BufferWithOwnMemory<WriteBuffer>(buffer_size_, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
		flush_buffer(BufferWithOwnMemory(buffer_size_, nullptr, DEFAULT_AIO_FILE_BLOCK_SIZE)),
		filename(filename_)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = (flags_ == -1) ? (O_WRONLY | O_TRUNC | O_CREAT) : flags_;
	open_flags |= O_DIRECT;

	fd = ::open(filename.c_str(), open_flags, mode_);
	if (fd == -1)
	{
		got_exception = true;
		auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
		throwFromErrno("Cannot open file " + filename, error_code);
	}

	fd2 = ::open(filename.c_str(), O_RDONLY, mode_);
	if (fd2 == -1)
	{
		got_exception = true;
		auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
		throwFromErrno("Cannot open file " + filename, error_code);
	}

	::memset(&request, 0, sizeof(request));
}

WriteBufferAIO::~WriteBufferAIO()
{
	if (!got_exception)
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
	if (fd2 != -1)
		::close(fd2);
}

off_t WriteBufferAIO::seek(off_t off, int whence)
{
	flush();

	if (whence == SEEK_SET)
	{
		if (off < 0)
		{
			got_exception = true;
			throw Exception("SEEK_SET underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
		pos_in_file = off;
	}
	else if (whence == SEEK_CUR)
	{
		if (off >= 0)
		{
			if (off > (std::numeric_limits<off_t>::max() - pos_in_file))
			{
				got_exception = true;
				throw Exception("SEEK_CUR overflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
			}
		}
		else if (off < -pos_in_file)
		{
			got_exception = true;
			throw Exception("SEEK_CUR underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
		pos_in_file += off;
	}
	else
	{
		got_exception = true;
		throw Exception("WriteBufferAIO::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	if (pos_in_file > max_pos)
		max_pos = pos_in_file;

	return pos_in_file;
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
	{
		got_exception = true;
		throwFromErrno("Cannot truncate file " + filename, ErrorCodes::CANNOT_TRUNCATE_FILE);
	}
}

void WriteBufferAIO::sync()
{
	flush();

	/// Попросим ОС сбросить данные на диск.
	int res = ::fsync(fd);
	if (res == -1)
	{
		got_exception = true;
		throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
	}
}

void WriteBufferAIO::flush()
{
	next();
	waitForAIOCompletion();
}

void WriteBufferAIO::nextImpl()
{
	if (!offset())
		return;

	waitForAIOCompletion();
	swapBuffers();

	truncate_count = 0;

	/// Регион диска, в который хотим записать данные.
	off_t region_begin = pos_in_file;
	off_t region_end = pos_in_file + flush_buffer.offset();
	size_t region_size = region_end - region_begin;

	/// Регион диска, в который действительно записываем данные.
	size_t region_left_padding = region_begin % DEFAULT_AIO_FILE_BLOCK_SIZE;
	size_t region_right_padding = 0;
	if (region_end % DEFAULT_AIO_FILE_BLOCK_SIZE != 0)
		region_right_padding = DEFAULT_AIO_FILE_BLOCK_SIZE - (region_end % DEFAULT_AIO_FILE_BLOCK_SIZE);

	off_t region_aligned_begin = region_begin - region_left_padding;
	off_t region_aligned_end = region_end + region_right_padding;
	size_t region_aligned_size = region_aligned_end - region_aligned_begin;

	/// Буфер данных, которые хотим записать на диск.
	Position buffer_begin = flush_buffer.buffer().begin();
	Position buffer_end = buffer_begin + region_size;
	size_t buffer_size = buffer_end - buffer_begin;
	size_t buffer_capacity = flush_buffer.buffer().size();

	/// Обработать буфер, чтобы он отражал структуру региона диска.

	size_t excess = 0;
	if (region_left_padding > 0)
	{
		if ((region_left_padding + buffer_size) > buffer_capacity)
		{
			excess = region_left_padding + buffer_size - buffer_capacity;
			::memset(&memory_page[0], 0, memory_page.size());
			::memcpy(&memory_page[0], buffer_end - excess, excess);
			buffer_end = buffer_begin + buffer_capacity;
			buffer_size = buffer_capacity;
		}
		else
		{
			buffer_size += region_left_padding;
			buffer_end = buffer_begin + buffer_size;
		}

		::memmove(buffer_begin + region_left_padding, buffer_begin, buffer_size - region_left_padding);
		::memset(buffer_begin, 0, region_left_padding);

		ssize_t read_count = ::pread(fd2, buffer_begin, region_left_padding, region_aligned_begin);
		if (read_count < 0)
		{
			got_exception = true;
			throw Exception("Read error", ErrorCodes::AIO_READ_ERROR);
		}
	}

	if (region_right_padding > 0)
	{
		Position end_ptr;
		if (excess > 0)
			end_ptr = &memory_page[excess];
		else
			end_ptr = buffer_end;

		::memset(end_ptr, 0, region_right_padding);
		ssize_t read_count = ::pread(fd2, end_ptr, region_right_padding, region_end);
		if (read_count < 0)
		{
			got_exception = true;
			throw Exception("Read error", ErrorCodes::AIO_READ_ERROR);
		}
		truncate_count = region_right_padding - read_count;
	}

	/// Создать запрос на асинхронную запись.

	size_t i =  0;

	iov[i].iov_base = buffer_begin;
	iov[i].iov_len = ((excess > 0) ? buffer_capacity : region_aligned_size);
	++i;

	if (excess > 0)
	{
		iov[i].iov_base = &memory_page[0];
		iov[i].iov_len = memory_page.size();
		++i;
	}

	bytes_to_write = 0;
	for (size_t j = 0; j < i; ++j)
	{
		if ((iov[j].iov_len > std::numeric_limits<off_t>::max()) ||
			(static_cast<off_t>(iov[j].iov_len) > (std::numeric_limits<off_t>::max() - bytes_to_write)))
		{
			got_exception = true;
			throw Exception("Overflow on bytes to write", ErrorCodes::LOGICAL_ERROR);
		}
		bytes_to_write += iov[j].iov_len;
	}

	request.aio_lio_opcode = IOCB_CMD_PWRITEV;
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

	is_pending_write = true;
}

void WriteBufferAIO::waitForAIOCompletion()
{
	if (is_pending_write)
	{
		while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
		{
			if (errno != EINTR)
			{
				got_exception = true;
				throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
			}
		}

		is_pending_write = false;
		off_t bytes_written = events[0].res;

		if (bytes_written < bytes_to_write)
		{
			got_exception = true;
			throw Exception("Asynchronous write error on file " + filename, ErrorCodes::AIO_WRITE_ERROR);
		}

		bytes_written -= truncate_count;
		if (pos_in_file > (std::numeric_limits<off_t>::max() - bytes_written))
		{
			got_exception = true;
			throw Exception("File position overflowed", ErrorCodes::LOGICAL_ERROR);
		}

		off_t delta = pos_in_file - request.aio_offset;
		pos_in_file += bytes_written - delta;

		if (pos_in_file > max_pos)
			max_pos = pos_in_file;

		if (truncate_count > 0)
		{
			// Delete the trailing zeroes that were added for alignment purposes.
			int res = ::ftruncate(fd, max_pos);
			if (res == -1)
			{
				got_exception = true;
				throwFromErrno("Cannot truncate file " + filename, ErrorCodes::CANNOT_TRUNCATE_FILE);
			}
		}
	}
}

void WriteBufferAIO::swapBuffers() noexcept
{
	buffer().swap(flush_buffer.buffer());
	std::swap(position(), flush_buffer.position());
}

}
