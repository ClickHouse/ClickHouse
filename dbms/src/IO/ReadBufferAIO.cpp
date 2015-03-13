#include <DB/IO/ReadBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Defines.h>

#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

ReadBufferAIO::ReadBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, mode_t mode_,
	char * existing_memory_)
	: BufferWithOwnMemory(buffer_size_, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
	fill_buffer(BufferWithOwnMemory(buffer_size_, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE)),
	request_ptrs{ &request }, events(1), filename(filename_)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = (flags_ == -1) ? O_RDONLY : flags_;
	open_flags |= O_DIRECT;

	fd = ::open(filename.c_str(), open_flags, mode_);
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
			waitForAIOCompletion();
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
	if ((max_bytes_read_ % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
	{
		got_exception = true;
		throw Exception("Invalid maximum number of bytes to read from file " + filename, ErrorCodes::AIO_UNALIGNED_SIZE_ERROR);
	}
	max_bytes_read = max_bytes_read_;
}

off_t ReadBufferAIO::seek(off_t off, int whence)
{
	if ((off % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
		throw Exception("Invalid offset for ReadBufferAIO::seek", ErrorCodes::AIO_UNALIGNED_SIZE_ERROR);

	waitForAIOCompletion();

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

	waitForAIOCompletion();

	/// При первом вызове не надо обменять местами основной и дублирующий буферы.
	if (is_started)
		swapBuffers();
	else
		is_started = true;

	/// Если конец файла только что достигнут, больше ничего не делаем.
	if (is_eof)
		return true;

	/// Создать запрос.
	request.aio_lio_opcode = IOCB_CMD_PREAD;
	request.aio_fildes = fd;
	request.aio_buf = reinterpret_cast<UInt64>(fill_buffer.internalBuffer().begin());
	request.aio_nbytes = std::min(fill_buffer.internalBuffer().size(), max_bytes_read);
	request.aio_offset = pos_in_file;
	request.aio_reqprio = 0;

	/// Отправить запрос.
	while (io_submit(aio_context.ctx, request_ptrs.size(), &request_ptrs[0]) < 0)
		if (errno != EINTR)
		{
			got_exception = true;
			throw Exception("Cannot submit request for asynchronous IO on file " + filename, ErrorCodes::AIO_SUBMIT_ERROR);
		}

	is_pending_read = true;
	return true;
}

void ReadBufferAIO::waitForAIOCompletion()
{
	if (is_pending_read)
	{
		while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
			if (errno != EINTR)
			{
				got_exception = true;
				throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
			}

		is_pending_read = false;
		off_t bytes_read = events[0].res;

		if (bytes_read < 0)
		{
			got_exception = true;
			throw Exception("Asynchronous read error on file " + filename, ErrorCodes::AIO_READ_ERROR);
		}
		if ((bytes_read % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
		{
			got_exception = true;
			throw Exception("Received unaligned number of bytes from file " + filename, ErrorCodes::AIO_UNALIGNED_SIZE_ERROR);
		}
		if (pos_in_file > (std::numeric_limits<off_t>::max() - bytes_read))
		{
			got_exception = true;
			throw Exception("File position overflowed", ErrorCodes::LOGICAL_ERROR);
		}

		pos_in_file += bytes_read;
		total_bytes_read += bytes_read;

		if (bytes_read > 0)
			fill_buffer.buffer().resize(bytes_read);
		if ((static_cast<size_t>(bytes_read) < fill_buffer.internalBuffer().size()) || (total_bytes_read == max_bytes_read))
			is_eof = true;
	}
}

void ReadBufferAIO::swapBuffers() noexcept
{
	internalBuffer().swap(fill_buffer.internalBuffer());
	buffer().swap(fill_buffer.buffer());
	std::swap(position(), fill_buffer.position());
}

}
