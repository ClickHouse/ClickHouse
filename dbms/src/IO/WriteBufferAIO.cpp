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

	/// About O_RDWR: yep, we really mean it.
	int open_flags = (flags_ == -1) ? (O_RDWR | O_TRUNC | O_CREAT) : flags_;
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

	/// Input parameters: fd, pos_in_file, flush_buffer

	/*
			region_aligned_begin     region_begin                             region_end      region_aligned_end
			|                           |                                          |                           |
			|     +---------------------+                                          +----------------------+    |
			|     |                                                                                       |    |
			+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
			|XXXXX*  :        :        :        :        :        :        :        :        :        :   *XXXX|
	+-------|XXXXX*  :        :        :        :        :        :        :        :        :        :   *XXXX|-------+
	|		|XXXXX*  :        :        :        :        :        :        :        :        :        :   *XXXX|       |
	|		|XXXXX*  :        :        :        :        :        :        :        :        :        :   *XXXX|       |
	|		+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+       |
(1)	|           |                                       ^                                                 |            |(1)
read|			+---- left padded disk page             |                      right padded disk page ----+            |read
    |                                                   |                                                              |
	|		+--------+ (left padded page)               |                        (right padded page)  +--------+       |
	|		|XXXXX*YY|                                  |                                             |ZZZ*XXXX|       |
	|		|XXXXX*YY|--------------------------------->+<--------------------------------------------|ZZZ*XXXX|       |
	+------>|XXXXX*YY|<--+                              | (3) scattered write           +------------>|ZZZ*XXXX|<------+
			|XXXXX*YY|   |                              |                               |             |ZZZ*XXXX|
			+--------+   |(2)copy                       |                               |(2)copy      +--------+
                         |                              |                               |
	+--------------------+                              |                               +--------------------+
	|                                                   |                                                    |
	|		buffer_begin     aligned_buffer_begin.......+...........aligned_buffer_end     buffer_end        |
	|		|                        |                                   |                          |        |
	|		|  +---------------------+                                   +----------------------+   |        |
	|		|  |                                                                                |   |        |
	|		---+--------+--------+--------+--------+--------+--------+--------+--------+--------+----        |
	|		*YY:        :        :        :        :        :        :        :        :        :ZZZ*        |
	|		*YY:        :        :        :        :        :        :        :        :        :ZZZ*        |
	+-------*YY:        :        :        :        :        :        :        :        :        :ZZZ*--------+
			*YY:        :        :        :        :        :        :        :        :        :ZZZ*
			---+--------+--------+--------+--------+--------+--------+--------+--------+--------+----

	 */

	//
	// 1. Determine the enclosing page-aligned disk region.
	//

	/// Disk region we want to write to.
	size_t region_begin = pos_in_file;
	size_t region_end = pos_in_file + flush_buffer.offset();

	/// Page-aligned disk region.
	size_t region_aligned_begin = region_begin - (region_begin % DEFAULT_AIO_FILE_BLOCK_SIZE);
	size_t region_aligned_end = region_end;
	if ((region_aligned_end % DEFAULT_AIO_FILE_BLOCK_SIZE) != 0)
		region_aligned_end += DEFAULT_AIO_FILE_BLOCK_SIZE - (region_aligned_end % DEFAULT_AIO_FILE_BLOCK_SIZE);

	bool has_left_padding = (region_aligned_begin < region_begin);
	bool has_right_padding = (region_aligned_end > region_end);

	//
	// 2. Read needed data from disk into padded pages.
	//

	if (has_left_padding)
	{
		/// Left-side padding disk region.
		::memset(&left_page[0], 0, left_page.size());
		ssize_t read_count = ::pread(fd, &left_page[0], DEFAULT_AIO_FILE_BLOCK_SIZE, region_aligned_begin);
		if (read_count < 0)
			throw Exception("Read error", ErrorCodes::AIO_READ_ERROR);
	}
	if (has_right_padding)
	{
		/// Right-side padding disk region.
		::memset(&right_page[0], 0, right_page.size());
		ssize_t read_count = ::pread(fd, &right_page[0], DEFAULT_AIO_FILE_BLOCK_SIZE, (region_aligned_end - DEFAULT_AIO_FILE_BLOCK_SIZE));
		if (read_count < 0)
			throw Exception("Read error", ErrorCodes::AIO_WRITE_ERROR);
		truncate_count = DEFAULT_AIO_FILE_BLOCK_SIZE - read_count;
	}

	//
	// 3. Copy padding data (2 user-space copies) from the buffer into the padded pages.
	//

	/// Buffer we want to write to disk.
	Position buffer_begin = flush_buffer.buffer().begin();
	Position buffer_end = buffer_begin + flush_buffer.offset();

	/// Subset of the buffer that is page-aligned.
	Position aligned_buffer_begin = buffer_begin;
	Position aligned_buffer_end = buffer_end;

	if (has_left_padding)
	{
		size_t left_page_unmodified_size = region_begin - region_aligned_begin;
		size_t left_page_modified_size = DEFAULT_AIO_FILE_BLOCK_SIZE - left_page_unmodified_size;
		aligned_buffer_begin += left_page_modified_size;
		::memcpy(&left_page[0] + left_page_unmodified_size, buffer_begin, left_page_modified_size);
	}
	if (has_right_padding)
	{
		size_t right_page_begin = region_aligned_end - DEFAULT_AIO_FILE_BLOCK_SIZE;
		size_t right_page_modified_size = region_end - right_page_begin;
		aligned_buffer_end -= right_page_modified_size;
		::memcpy(&right_page[0], (buffer_end - right_page_modified_size), right_page_modified_size);
	}

	//
	// 4. Create requests.
	//

	size_t i = 0;

	if (has_left_padding)
	{
		iov[i].iov_base = &left_page[0];
		iov[i].iov_len = DEFAULT_AIO_FILE_BLOCK_SIZE;
		++i;
	}

	iov[i].iov_base = aligned_buffer_begin;
	iov[i].iov_len = aligned_buffer_end - aligned_buffer_begin;
	++i;

	if (has_right_padding)
	{
		iov[i].iov_base = &right_page[0];
		iov[i].iov_len = DEFAULT_AIO_FILE_BLOCK_SIZE;
		++i;
	}

	bytes_to_write = 0;
	for (size_t j = 0; j < i; ++j)
	{
		if ((iov[i].iov_len > std::numeric_limits<off_t>::max()) ||
			(static_cast<off_t>(iov[i].iov_len) > (std::numeric_limits<off_t>::max() - bytes_to_write)))
		{
			got_exception = true;
			throw Exception("Overflow on bytes to write", ErrorCodes::LOGICAL_ERROR);
		}
		bytes_to_write += iov[i].iov_len;
	}

	/// Send requests (1 syscall).
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

		// Delete the trailing zeroes that were added for alignment purposes.
		if (truncate_count > 0)
		{
			bytes_written -= truncate_count;

			int res = ::ftruncate(fd, truncate_count);
			if (res == -1)
			{
				got_exception = true;
				throwFromErrno("Cannot truncate file " + filename, ErrorCodes::CANNOT_TRUNCATE_FILE);
			}
		}

		if (pos_in_file > (std::numeric_limits<off_t>::max() - bytes_written))
		{
			got_exception = true;
			throw Exception("File position overflowed", ErrorCodes::LOGICAL_ERROR);
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
