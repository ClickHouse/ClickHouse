#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <memory>

namespace DB
{

class OwningBufferBlockInputStream : public IProfilingBlockInputStream
{
public:
	OwningBufferBlockInputStream(const BlockInputStreamPtr & stream, std::unique_ptr<ReadBuffer> buffer)
		: stream{stream}, buffer{std::move(buffer)}
	{
		children.push_back(stream);
	}

private:
	Block readImpl() override { return stream->read(); }

	String getName() const override { return "OwningBufferBlockInputStream"; }

	String getID() const override {
		return "OwningBuffer(" + stream->getID() + ")";
	}

	BlockInputStreamPtr stream;
	std::unique_ptr<ReadBuffer> buffer;
};

}
