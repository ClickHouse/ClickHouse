#include <DB/IO/ReadBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>

#include <cerrno>
#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

ReadBufferAIO::ReadBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, mode_t mode_,
	char * existing_memory_)
	: BufferWithOwnMemory(buffer_size_, existing_memory_, BLOCK_SIZE),
	fill_buffer(BufferWithOwnMemory(buffer_size_, existing_memory_, BLOCK_SIZE)),
	filename(filename_), request_ptrs{ &cb }, events(1)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = ((flags_ == -1) ? O_RDONLY : flags_);
	open_flags |= O_DIRECT;

	fd = ::open(filename.c_str(), open_flags, mode_);
	if (fd == -1)
	{
		got_exception = true;
		throwFromErrno("Cannot open file " + filename, (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
	}
}

ReadBufferAIO::~ReadBufferAIO()
{
	if (!got_exception)
	{
		try
		{
			waitForCompletion();
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
	if ((max_bytes_read_ % BLOCK_SIZE) != 0)
	{
		got_exception = true;
		throw Exception("Invalid maximum number of bytes to read from file " + filename, ErrorCodes::AIO_UNALIGNED_BUFFER_ERROR);
	}
	max_bytes_read = max_bytes_read_;
}

/// Если offset такой маленький, что мы не выйдем за пределы буфера, настоящий seek по файлу не делается.
off_t ReadBufferAIO::seek(off_t off, int whence)
{
	if ((off % DB::ReadBufferAIO::BLOCK_SIZE) != 0)
		throw Exception("Invalid offset for ReadBufferAIO::seek", ErrorCodes::AIO_UNALIGNED_BUFFER_ERROR);

	waitForCompletion();

	off_t new_pos = off;
	if (whence == SEEK_CUR)
		new_pos = pos_in_file - (working_buffer.end() - pos) + off;
	else if (whence != SEEK_SET)
	{
		got_exception = true;
		throw Exception("ReadBufferAIO::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	/// Никуда не сдвинулись.
	if ((new_pos + (working_buffer.end() - pos)) == pos_in_file)
		return new_pos;

	if (hasPendingData() && (new_pos <= pos_in_file) && (new_pos >= (pos_in_file - static_cast<off_t>(working_buffer.size()))))
	{
		/// Остались в пределах буфера.
		pos = working_buffer.begin() + (new_pos - (pos_in_file - working_buffer.size()));
		return new_pos;
	}
	else
	{
		ProfileEvents::increment(ProfileEvents::Seek);

		pos = working_buffer.end();
		off_t res = ::lseek(fd, new_pos, SEEK_SET);
		if (res == -1)
		{
			got_exception = true;
			throwFromErrno("Cannot seek through file " + filename, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
		}
		pos_in_file = new_pos;
		return res;
	}
}

bool ReadBufferAIO::nextImpl()
{
	if (is_eof)
		return false;

	waitForCompletion();

	if (is_started)
		swapBuffers();

	if (is_eof)
		return true;

	// Create request.
	::memset(&cb, 0, sizeof(cb));

	cb.aio_lio_opcode = IOCB_CMD_PREAD;
	cb.aio_fildes = fd;
	cb.aio_buf = reinterpret_cast<UInt64>(fill_buffer.internalBuffer().begin());
	cb.aio_nbytes = std::min(fill_buffer.internalBuffer().size(), max_bytes_read);
	cb.aio_offset = pos_in_file;
	cb.aio_reqprio = 0;

	// Submit request.
	while (io_submit(aio_context.ctx, request_ptrs.size(), &request_ptrs[0]) < 0)
		if (errno != EINTR)
		{
			got_exception = true;
			throw Exception("Cannot submit request for asynchronous IO on file " + filename, ErrorCodes::AIO_SUBMIT_ERROR);
		}

	is_pending_read = true;

	if (!is_started)
		is_started = true;

	return true;
}

void ReadBufferAIO::waitForCompletion()
{
	if (is_pending_read)
	{
		::memset(&events[0], 0, sizeof(events[0]) * events.size());

		while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
			if (errno != EINTR)
			{
				got_exception = true;
				throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
			}

		is_pending_read = false;

		size_t bytes_read = (events[0].res > 0) ? static_cast<size_t>(events[0].res) : 0;
		if ((bytes_read % BLOCK_SIZE) != 0)
		{
			got_exception = true;
			throw Exception("Received unaligned number of bytes from file " + filename, ErrorCodes::AIO_UNALIGNED_BUFFER_ERROR);
		}

		pos_in_file += bytes_read;
		total_bytes_read += bytes_read;

		if (bytes_read > 0)
			fill_buffer.buffer().resize(bytes_read);
		if ((bytes_read < fill_buffer.internalBuffer().size()) || (total_bytes_read == max_bytes_read))
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
