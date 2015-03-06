#pragma once

#include <string>

namespace DB
{

class IBufferAIO
{
public:
	IBufferAIO() = default;
	virtual ~IBufferAIO() = default;
	virtual std::string getFileName() const noexcept = 0;
	virtual int getFD() const noexcept = 0;

public:
	static const size_t BLOCK_SIZE = 512;

protected:
	virtual void waitForCompletion() = 0;
	virtual void swapBuffers() noexcept = 0;
};

}
