#include <DB/IO/WriteBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Defines.h>

#include <limits>
#include <sys/types.h>
#include <sys/stat.h>

namespace DB
{

WriteBufferAIO::WriteBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, mode_t mode_,
		char * existing_memory_)
		: BufferWithOwnMemory(buffer_size_, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
		flush_buffer(BufferWithOwnMemory(buffer_size_, nullptr, DEFAULT_AIO_FILE_BLOCK_SIZE)),
		request_ptrs{ &request }, events(1), filename(filename_)
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

	::memset(&request, 0, sizeof(request));
}

WriteBufferAIO::~WriteBufferAIO()
{
	if (!got_exception)
	{
		try
		{
			/// Если в буфере ещё остались данные - запишем их.
			next();
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

off_t WriteBufferAIO::seek(off_t off, int whence)
{
	if ((off % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
		throw Exception("Invalid offset for WriteBufferAIO::seek", ErrorCodes::AIO_UNALIGNED_SIZE_ERROR);

	/// Если в буфере ещё остались данные - запишем их.
	next();
	waitForCompletion();

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

	return pos_in_file;
}

off_t WriteBufferAIO::getPositionInFile()
{
	return seek(0, SEEK_CUR);
}

void WriteBufferAIO::truncate(off_t length)
{
	if ((length % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
		throw Exception("Invalid length for WriteBufferAIO::ftruncate", ErrorCodes::AIO_UNALIGNED_SIZE_ERROR);

	/// Если в буфере ещё остались данные - запишем их.
	next();
	waitForCompletion();

	int res = ::ftruncate(fd, length);
	if (res == -1)
	{
		got_exception = true;
		throwFromErrno("Cannot truncate file " + filename, ErrorCodes::CANNOT_TRUNCATE_FILE);
	}
}

void WriteBufferAIO::sync()
{
	/// Если в буфере ещё остались данные - запишем их.
	next();
	waitForCompletion();

	/// Попросим ОС сбросить данные на диск.
	int res = ::fsync(fd);
	if (res == -1)
	{
		got_exception = true;
		throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
	}
}

void WriteBufferAIO::nextImpl()
{
	if (!offset())
		return;

	waitForCompletion();
	swapBuffers();

	/// Создать запрос.
	request.aio_lio_opcode = IOCB_CMD_PWRITE;
	request.aio_fildes = fd;
	request.aio_buf = reinterpret_cast<UInt64>(flush_buffer.buffer().begin());
	request.aio_nbytes = flush_buffer.offset();
	request.aio_offset = pos_in_file;
	request.aio_reqprio = 0;

	if ((request.aio_nbytes % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
	{
		got_exception = true;
		throw Exception("Illegal attempt to write unaligned data to file " + filename, ErrorCodes::AIO_UNALIGNED_SIZE_ERROR);
	}

	/// Отправить запрос.
	while (io_submit(aio_context.ctx, request_ptrs.size(), &request_ptrs[0]) < 0)
		if (errno != EINTR)
		{
			got_exception = true;
			throw Exception("Cannot submit request for asynchronous IO on file " + filename, ErrorCodes::AIO_SUBMIT_ERROR);
		}

	is_pending_write = true;
}

void WriteBufferAIO::waitForCompletion()
{
	if (is_pending_write)
	{
		while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
			if (errno != EINTR)
			{
				got_exception = true;
				throw Exception("Failed to wait for asynchronous IO completion on file " + filename, ErrorCodes::AIO_COMPLETION_ERROR);
			}

		is_pending_write = false;

		if (events[0].res < 0)
		{
			got_exception = true;
			throw Exception("Asynchronous write error on file " + filename, ErrorCodes::AIO_WRITE_ERROR);
		}

		size_t bytes_written = static_cast<size_t>(events[0].res);
		if (bytes_written < flush_buffer.offset())
		{
			got_exception = true;
			throw Exception("Asynchronous write error on file " + filename, ErrorCodes::AIO_WRITE_ERROR);
		}

		pos_in_file += bytes_written;
	}
}

void WriteBufferAIO::swapBuffers() noexcept
{
	buffer().swap(flush_buffer.buffer());
	std::swap(position(), flush_buffer.position());
}

}
