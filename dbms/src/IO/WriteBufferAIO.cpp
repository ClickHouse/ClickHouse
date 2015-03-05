#include <DB/IO/WriteBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace DB
{

WriteBufferAIO::WriteBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, mode_t mode_,
		char * existing_memory_)
		: BufferWithOwnMemory(buffer_size_, existing_memory_, BLOCK_SIZE),
		flush_buffer(BufferWithOwnMemory(buffer_size_, nullptr, BLOCK_SIZE)),
		filename(filename_), request_ptrs{ &cb }, events(1)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = ((flags_ == -1) ? O_WRONLY | O_TRUNC | O_CREAT : flags_);
	open_flags |= O_DIRECT;

	fd = ::open(filename_.c_str(), open_flags, mode_);
	if (fd == -1)
		throwFromErrno("Cannot open file " + filename_, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

WriteBufferAIO::~WriteBufferAIO()
{
	try
	{
		next();
		waitForCompletion();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	::close(fd);
}

void WriteBufferAIO::sync()
{
	::fsync(fd);
}

void WriteBufferAIO::nextImpl()
{
	if (!offset())
		return;

	waitForCompletion();
	swapBuffers();

	// Create request.
	::memset(&cb, 0, sizeof(cb));

	cb.aio_lio_opcode = IOCB_CMD_PWRITE;
	cb.aio_fildes = fd;
	cb.aio_buf = reinterpret_cast<UInt64>(flush_buffer.buffer().begin());
	cb.aio_nbytes = flush_buffer.offset();
	cb.aio_offset = 0;
	cb.aio_reqprio = 0;

	// Submit request.
	while (io_submit(aio_context.ctx, request_ptrs.size(), &request_ptrs[0]) < 0)
		if (errno != EINTR)
			throw Exception("Cannot submit request for asynchronous IO", ErrorCodes::AIO_SUBMIT_ERROR);

	is_pending_write = true;
}

void WriteBufferAIO::waitForCompletion()
{
	if (is_pending_write)
	{
		::memset(&events[0], 0, sizeof(events[0]) * events.size());

		while (io_getevents(aio_context.ctx, events.size(), events.size(), &events[0], nullptr) < 0)
			if (errno != EINTR)
				throw Exception("Failed to wait for asynchronous IO completion", ErrorCodes::AIO_COMPLETION_ERROR);

		is_pending_write = false;
	}
}

void WriteBufferAIO::swapBuffers()
{
	buffer().swap(flush_buffer.buffer());
	std::swap(position(), flush_buffer.position());
}

}
