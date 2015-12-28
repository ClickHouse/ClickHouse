#include <DB/IO/ReadBufferAIO.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/Stopwatch.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Defines.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <experimental/optional>


namespace DB
{

/// Примечание: выделяется дополнительная страница, которая содежрит те данные, которые
/// не влезают в основной буфер.
ReadBufferAIO::ReadBufferAIO(const std::string & filename_, size_t buffer_size_, int flags_, char * existing_memory_)
	: ReadBufferFromFileBase(buffer_size_ + DEFAULT_AIO_FILE_BLOCK_SIZE, existing_memory_, DEFAULT_AIO_FILE_BLOCK_SIZE),
	  fill_buffer(BufferWithOwnMemory<ReadBuffer>(internalBuffer().size(), nullptr, DEFAULT_AIO_FILE_BLOCK_SIZE)),
	  filename(filename_)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	int open_flags = (flags_ == -1) ? O_RDONLY : flags_;
	open_flags |= O_DIRECT;

	fd = ::open(filename.c_str(), open_flags);
	if (fd == -1)
	{
		auto error_code = (errno == ENOENT) ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE;
		throwFromErrno("Cannot open file " + filename, error_code);
	}
}

ReadBufferAIO::~ReadBufferAIO()
{
	if (!aio_failed)
	{
		try
		{
			(void) waitForAIOCompletion();
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
		throw Exception("Illegal attempt to set the maximum number of bytes to read from file " + filename, ErrorCodes::LOGICAL_ERROR);
	max_bytes_read = max_bytes_read_;
}

bool ReadBufferAIO::nextImpl()
{
	/// Если конец файла уже был достигнут при вызове этой функции,
	/// то текущий вызов ошибочен.
	if (is_eof)
		return false;

	std::experimental::optional<Stopwatch> watch;
	if (profile_callback)
		watch.emplace(clock_type);

	if (!is_aio)
	{
		synchronousRead();
		is_aio = true;
	}
	else
		receive();

	if (profile_callback)
	{
		ProfileInfo info;
		info.bytes_requested = requested_byte_count;
		info.bytes_read = bytes_read;
		info.nanoseconds = watch->elapsed();
		profile_callback(info);
	}

	is_started = true;

	/// Если конец файла только что достигнут, больше ничего не делаем.
	if (is_eof)
		return true;

	/// Создать асинхронный запрос.
	prepare();

	request.aio_lio_opcode = IOCB_CMD_PREAD;
	request.aio_fildes = fd;
	request.aio_buf = reinterpret_cast<UInt64>(buffer_begin);
	request.aio_nbytes = region_aligned_size;
	request.aio_offset = region_aligned_begin;

	/// Отправить запрос.
	try
	{
		future_bytes_read = AIOContextPool::instance().post(request);
	}
	catch (...)
	{
		aio_failed = true;
		throw;
	}

	is_pending_read = true;
	return true;
}

off_t ReadBufferAIO::doSeek(off_t off, int whence)
{
	off_t new_pos_in_file;

	if (whence == SEEK_SET)
	{
		if (off < 0)
			throw Exception("SEEK_SET underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		new_pos_in_file = off;
	}
	else if (whence == SEEK_CUR)
	{
		if (off >= 0)
		{
			if (off > (std::numeric_limits<off_t>::max() - getPositionInFile()))
				throw Exception("SEEK_CUR overflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
		else if (off < -getPositionInFile())
			throw Exception("SEEK_CUR underflow", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		new_pos_in_file = getPositionInFile() + off;
	}
	else
		throw Exception("ReadBufferAIO::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

	if (new_pos_in_file != getPositionInFile())
	{
		off_t first_read_pos_in_file = first_unread_pos_in_file - static_cast<off_t>(working_buffer.size());
		if (hasPendingData() && (new_pos_in_file >= first_read_pos_in_file) && (new_pos_in_file <= first_unread_pos_in_file))
		{
			/// Свдинулись, но остались в пределах буфера.
			pos = working_buffer.begin() + (new_pos_in_file - first_read_pos_in_file);
		}
		else
		{
			/// Сдвинулись за пределы буфера.
			pos = working_buffer.end();
			first_unread_pos_in_file = new_pos_in_file;

			/// Не можем использовать результат текущего асинхронного запроса.
			skip();
		}
	}

	return new_pos_in_file;
}

void ReadBufferAIO::synchronousRead()
{
	prepare();
	bytes_read = ::pread(fd, buffer_begin, region_aligned_size, region_aligned_begin);

	ProfileEvents::increment(ProfileEvents::ReadBufferAIORead);
	ProfileEvents::increment(ProfileEvents::ReadBufferAIOReadBytes, bytes_read);

	finalize();
}

void ReadBufferAIO::receive()
{
	if (!waitForAIOCompletion())
		return;
	finalize();
}

void ReadBufferAIO::skip()
{
	if (!waitForAIOCompletion())
		return;

	is_aio = false;

	/// @todo I presume this assignment is redundant since waitForAIOCompletion() performs a similar one
//	bytes_read = future_bytes_read.get();
	if ((bytes_read < 0) || (static_cast<size_t>(bytes_read) < region_left_padding))
		throw Exception("Asynchronous read error on file " + filename, ErrorCodes::AIO_READ_ERROR);
}

bool ReadBufferAIO::waitForAIOCompletion()
{
	if (is_eof || !is_pending_read)
		return false;

	bytes_read = future_bytes_read.get();
	is_pending_read = false;

	ProfileEvents::increment(ProfileEvents::ReadBufferAIORead);
	ProfileEvents::increment(ProfileEvents::ReadBufferAIOReadBytes, bytes_read);

	return true;
}

void ReadBufferAIO::prepare()
{
	requested_byte_count = std::min(fill_buffer.internalBuffer().size() - DEFAULT_AIO_FILE_BLOCK_SIZE, max_bytes_read);

	/// Регион диска, из которого хотим читать данные.
	const off_t region_begin = first_unread_pos_in_file;

	if ((requested_byte_count > std::numeric_limits<off_t>::max()) ||
		(first_unread_pos_in_file > (std::numeric_limits<off_t>::max() - static_cast<off_t>(requested_byte_count))))
		throw Exception("An overflow occurred during file operation", ErrorCodes::LOGICAL_ERROR);

	const off_t region_end = first_unread_pos_in_file + requested_byte_count;

	/// Выровненный регион диска, из которого будем читать данные.
	region_left_padding = region_begin % DEFAULT_AIO_FILE_BLOCK_SIZE;
	const size_t region_right_padding = (DEFAULT_AIO_FILE_BLOCK_SIZE - (region_end % DEFAULT_AIO_FILE_BLOCK_SIZE)) % DEFAULT_AIO_FILE_BLOCK_SIZE;

	region_aligned_begin = region_begin - region_left_padding;

	if (region_end > (std::numeric_limits<off_t>::max() - static_cast<off_t>(region_right_padding)))
		throw Exception("An overflow occurred during file operation", ErrorCodes::LOGICAL_ERROR);

	const off_t region_aligned_end = region_end + region_right_padding;
	region_aligned_size = region_aligned_end - region_aligned_begin;

	buffer_begin = fill_buffer.internalBuffer().begin();
}

void ReadBufferAIO::finalize()
{
	if ((bytes_read < 0) || (static_cast<size_t>(bytes_read) < region_left_padding))
		throw Exception("Asynchronous read error on file " + filename, ErrorCodes::AIO_READ_ERROR);

	/// Игнорируем излишние байты слева.
	bytes_read -= region_left_padding;

	/// Игнорируем излишние байты справа.
	bytes_read = std::min(bytes_read, static_cast<off_t>(requested_byte_count));

	if (bytes_read > 0)
		fill_buffer.buffer().resize(region_left_padding + bytes_read);
	if (static_cast<size_t>(bytes_read) < requested_byte_count)
		is_eof = true;

	if (first_unread_pos_in_file > (std::numeric_limits<off_t>::max() - bytes_read))
		throw Exception("An overflow occurred during file operation", ErrorCodes::LOGICAL_ERROR);

	first_unread_pos_in_file += bytes_read;
	total_bytes_read += bytes_read;
	working_buffer_offset = region_left_padding;

	if (total_bytes_read == max_bytes_read)
		is_eof = true;

	/// Менять местами основной и дублирующий буферы.
	internalBuffer().swap(fill_buffer.internalBuffer());
	buffer().swap(fill_buffer.buffer());
	std::swap(position(), fill_buffer.position());
}

}
