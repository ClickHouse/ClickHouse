#include <functional>
#include <forward_list>
#include <iostream>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/Core/Defines.h>
#include <DB/Common/Allocator.h>
#include <DB/Common/Exception.h>

#include <Poco/File.h>
#include <Poco/Path.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int CURRENT_WRITE_BUFFER_IS_EXHAUSTED;
	extern const int MEMORY_LIMIT_EXCEEDED;
	extern const int LOGICAL_ERROR;
}

class CascadeWriteBuffer : public WriteBuffer
{
public:

	using WriteBufferPtrs = std::vector<WriteBufferPtr>;
	using WriteBufferConstructor = std::function<WriteBufferPtr (const WriteBufferPtr & prev_buf)>;
	using WriteBufferConstructors = std::vector<WriteBufferConstructor>;

	CascadeWriteBuffer(WriteBufferPtrs && prepared_sources_, WriteBufferConstructors && lazy_sources_ = {});

	void nextImpl() override;

	/// Should be called once
	void getResultBuffers(WriteBufferPtrs & res);

private:

	WriteBuffer * getNextBuffer();

	WriteBufferPtrs prepared_sources;
	WriteBufferConstructors lazy_sources;
	size_t first_lazy_source_num;
	size_t num_sources;

	WriteBuffer * curr_buffer;
	size_t curr_buffer_num;
};


class ReadBufferFromMemoryWriteBuffer;
class ReadBufferFromTemporaryWriteBuffer;


struct IReadableWriteBuffer
{
	/// Creates read buffer from current write buffer
	/// Returned buffer points to the first byte of original buffer and finds with current position.
	/// Original stream becomes invalid.
	virtual std::shared_ptr<ReadBuffer> getReadBuffer() = 0;

	virtual ~IReadableWriteBuffer() {}
};


/// Allow to write large data into memory
class MemoryWriteBuffer : public WriteBuffer, public IReadableWriteBuffer
{
public:

	MemoryWriteBuffer(
		size_t max_total_size_ = 0,
		size_t initial_chunk_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		double growth_rate_ = 2.0);

	void nextImpl() override;

	std::shared_ptr<ReadBuffer> getReadBuffer() override;

	~MemoryWriteBuffer() override;

protected:

	const size_t max_total_size;
	const size_t initial_chunk_size;
	const double growth_rate;

	using Container = std::forward_list<BufferBase::Buffer>;

	Container chunk_list;
	Container::iterator chunk_tail;
	size_t total_chunks_size = 0;

	void addChunk();

	friend class ReadBufferFromMemoryWriteBuffer;
};


/// Rereadable WriteBuffer, could be used as disk buffer
/// Creates unique temporary in directory (and directory itself)
class WriteBufferFromTemporaryFile : public WriteBufferFromFile, public IReadableWriteBuffer
{
public:
	using Ptr = std::shared_ptr<WriteBufferFromTemporaryFile>;

	/// path_template examle "/opt/clickhouse/tmp/data.XXXXXX"
	static Ptr create(const std::string & path_template_);

	std::shared_ptr<ReadBuffer> getReadBuffer() override;

	~WriteBufferFromTemporaryFile() override;

protected:

	WriteBufferFromTemporaryFile(int fd, const std::string & tmp_path)
	: WriteBufferFromFile(fd, tmp_path)
	{}
};


}
