#pragma once
#include <memory>
#include <DB/IO/ReadBuffer.h>

namespace DB
{

struct IReadableWriteBuffer
{
	/// Creates read buffer from current write buffer.
	/// Returned buffer points to the first byte of original buffer.
	/// Original stream becomes invalid.
	virtual std::shared_ptr<ReadBuffer> getReadBuffer() = 0;

	virtual ~IReadableWriteBuffer() {}
};

}
