#include <DB/IO/MemoryReadWriteBuffer.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/Common/Exception.h>
#include <common/likely.h>
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
	extern const int CURRENT_WRITE_BUFFER_IS_EXHAUSTED;
	extern const int MEMORY_LIMIT_EXCEEDED;
	extern const int LOGICAL_ERROR;
	extern const int CANNOT_OPEN_FILE;
	extern const int CANNOT_SEEK_THROUGH_FILE;
	extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
	extern const int CANNOT_CREATE_IO_BUFFER;
}


inline std::ostream & operator << (std::ostream & stream, BufferBase & buffer)
{
	stream
	<< " begin=" << reinterpret_cast<void*>(buffer.buffer().begin())
	<< " pos=" << buffer.position() - buffer.buffer().begin()
	<< " size=" << buffer.buffer().size()
	<< " int_size=" << buffer.internalBuffer().size() << "\n";

	return stream;
}


CascadeWriteBuffer::CascadeWriteBuffer(WriteBufferPtrs && prepared_sources_, WriteBufferConstructors && lazy_sources_)
	: WriteBuffer(nullptr, 0), prepared_sources(std::move(prepared_sources_)), lazy_sources(std::move(lazy_sources_))
{
	first_lazy_source_num = prepared_sources.size();
	num_sources = first_lazy_source_num + lazy_sources.size();

	/// fill lazy sources by nullptr
	prepared_sources.resize(num_sources);

	curr_buffer_num = 0;
	curr_buffer = getNextBuffer();
	set(curr_buffer->buffer().begin(), curr_buffer->buffer().size());
}


void CascadeWriteBuffer::nextImpl()
{
	try
	{
		curr_buffer->position() = position();
		curr_buffer->next();
	}
	catch (const Exception & e)
	{
		if (curr_buffer_num < num_sources && e.code() == ErrorCodes::CURRENT_WRITE_BUFFER_IS_EXHAUSTED)
		{
			/// actualize position of old buffer (it was reset by WriteBuffer::next)
			curr_buffer->position() = position();

			/// good situation, fetch next WriteBuffer
			++curr_buffer_num;
			curr_buffer = getNextBuffer();
		}
		else
			throw;
	}

	set(curr_buffer->buffer().begin(), curr_buffer->buffer().size());
}


void CascadeWriteBuffer::getResultBuffers(WriteBufferPtrs & res)
{
	curr_buffer->position() = position();
	res = std::move(prepared_sources);

	curr_buffer = nullptr;
	curr_buffer_num = num_sources = 0;
}


WriteBuffer * CascadeWriteBuffer::getNextBuffer()
{
	if (first_lazy_source_num <= curr_buffer_num && curr_buffer_num < num_sources)
	{
		if (!prepared_sources[curr_buffer_num])
		{
			WriteBufferPtr prev_buf = (curr_buffer_num > 0) ? prepared_sources[curr_buffer_num - 1] : nullptr;
			prepared_sources[curr_buffer_num] = lazy_sources[curr_buffer_num - first_lazy_source_num](prev_buf);
		}
	}
	else if (curr_buffer_num >= num_sources)
		throw Exception("There are no WriteBuffers to write result", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

	WriteBuffer * res = prepared_sources[curr_buffer_num].get();
	if (!res)
		throw Exception("Required WriteBuffer is not created", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

	return res;
}


class ReadBufferFromMemoryWriteBuffer : public ReadBuffer
{
public:

	ReadBufferFromMemoryWriteBuffer(MemoryWriteBuffer && origin)
	:
	ReadBuffer(nullptr, 0),
	chunk_list(std::move(origin.chunk_list)),
	end_pos(origin.position())
	{
		chunk_head = chunk_list.begin();
		setChunk();
	}

	bool nextImpl() override
	{
		if (chunk_head == chunk_list.end())
			return false;

		++chunk_head;
		return setChunk();
	}

	~ReadBufferFromMemoryWriteBuffer()
	{
		for (const auto & range : chunk_list)
			Allocator<false>().free(range.begin(), range.size());
	}

private:

	/// update buffers and position according to chunk_head pointer
	bool setChunk()
	{
		if (chunk_head != chunk_list.end())
		{
			internalBuffer() = *chunk_head;

			auto next_chunk = chunk_head;
			++next_chunk;

			/// It is last chunk, it should be truncated
			if (next_chunk != chunk_list.end())
				buffer() = internalBuffer();
			else
				buffer() = Buffer(internalBuffer().begin(), end_pos);

			position() = buffer().begin();
		}
		else
		{
			buffer() = internalBuffer() = Buffer(nullptr, nullptr);
			position() = nullptr;
		}

		return buffer().size() != 0;
	}

	using Container = std::forward_list<BufferBase::Buffer>;

	Container chunk_list;
	Container::iterator chunk_head;
	Position end_pos;
};



MemoryWriteBuffer::MemoryWriteBuffer(size_t max_total_size_, size_t initial_chunk_size_, double growth_rate_)
: WriteBuffer(nullptr, 0), max_total_size(max_total_size_), initial_chunk_size(initial_chunk_size_), growth_rate(growth_rate_)
{
	addChunk();
}

void MemoryWriteBuffer::nextImpl()
{
	if (unlikely(hasPendingData()))
	{
		/// ignore flush
		buffer() = Buffer(pos, buffer().end());
		return;
	}

	addChunk();
}

void MemoryWriteBuffer::addChunk()
{
	size_t next_chunk_size;
	if (chunk_list.empty())
	{
		chunk_tail = chunk_list.before_begin();
		next_chunk_size = initial_chunk_size;
	}
	else
	{
		next_chunk_size = std::max(1ul, static_cast<size_t>(chunk_tail->size() * growth_rate));
	}

	if (max_total_size)
	{
		if (total_chunks_size + next_chunk_size > max_total_size)
			next_chunk_size = max_total_size - total_chunks_size;

		if (0 == next_chunk_size)
			throw Exception("MemoryWriteBuffer limit is exhausted", ErrorCodes::CURRENT_WRITE_BUFFER_IS_EXHAUSTED);
	}

	Position begin = reinterpret_cast<Position>(Allocator<false>().alloc(next_chunk_size));
	chunk_tail = chunk_list.emplace_after(chunk_tail, begin, begin + next_chunk_size);
	total_chunks_size += next_chunk_size;

	set(chunk_tail->begin(), chunk_tail->size());
}

std::shared_ptr<ReadBuffer> MemoryWriteBuffer::getReadBuffer()
{
	auto res = std::make_shared<ReadBufferFromMemoryWriteBuffer>(std::move(*this));

	/// invalidate members
	chunk_list.clear();
	chunk_tail = chunk_list.begin();

	return res;
}

MemoryWriteBuffer::~MemoryWriteBuffer()
{
	for (const auto & range : chunk_list)
		Allocator<false>().free(range.begin(), range.size());
}


WriteBufferFromTemporaryFile::Ptr WriteBufferFromTemporaryFile::create(const std::string & path_template_)
{
	std::string path_template = path_template_;

	if (path_template.empty() || path_template.back() != 'X')
		path_template += "XXXXXX";

	Poco::File(Poco::Path(path_template).makeParent()).createDirectories();

	int fd = mkstemp(const_cast<char *>(path_template.c_str()));
	if (fd < 0)
		throw Exception("Cannot create temporary file " + path_template, ErrorCodes::CANNOT_OPEN_FILE);

	return Ptr(new WriteBufferFromTemporaryFile(fd, path_template));
}


WriteBufferFromTemporaryFile::~WriteBufferFromTemporaryFile()
{
	/// remove temporary file if it was not passed to ReadBuffer
	if (getFD() >= 0 && !getFileName().empty())
	{
		Poco::File(getFileName()).remove();
	}
}


class ReadBufferFromTemporaryWriteBuffer : public ReadBufferFromFile
{
public:

	static ReadBufferPtr createFrom(WriteBufferFromTemporaryFile * origin)
	{
		int fd = origin->getFD();
		std::string file_name = origin->getFileName();

		off_t res = lseek(fd, 0, SEEK_SET);
		if (-1 == res)
			throwFromErrno("Cannot reread temporary file " + file_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

		return std::make_shared<ReadBufferFromTemporaryWriteBuffer>(fd, file_name);
	}

	ReadBufferFromTemporaryWriteBuffer(int fd, const std::string & file_name)
	: ReadBufferFromFile(fd, file_name)
	{}

	~ReadBufferFromTemporaryWriteBuffer() override
	{
		/// remove temporary file
		Poco::File(file_name).remove();
	}
};

ReadBufferPtr WriteBufferFromTemporaryFile::getReadBuffer()
{
	/// ignore buffer, write all data to file and reread it from disk
	sync();

	auto res = ReadBufferFromTemporaryWriteBuffer::createFrom(this);

	/// invalidate FD to avoid close(fd) in destructor
	setFD(-1);
	file_name = {};

	return res;
}


}
