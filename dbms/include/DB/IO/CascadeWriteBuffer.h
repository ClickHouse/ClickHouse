#pragma once
#include <functional>
#include <DB/IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CURRENT_WRITE_BUFFER_IS_EXHAUSTED;
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

	const WriteBuffer * getCurrentBuffer() const
	{
		return curr_buffer;
	}

	~CascadeWriteBuffer();

private:

	WriteBuffer * setNextBuffer();

	WriteBufferPtrs prepared_sources;
	WriteBufferConstructors lazy_sources;
	size_t first_lazy_source_num;
	size_t num_sources;

	WriteBuffer * curr_buffer;
	size_t curr_buffer_num;
};

}
