#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/** Прибавляет к одному потоку дополнительную информацию о блоках, которая задана
  * в качестве параметра конструктора.
  */
class BlockExtraInfoInputStream : public IProfilingBlockInputStream
{
public:
	BlockExtraInfoInputStream(BlockInputStreamPtr input_, const BlockExtraInfo & block_extra_info_)
		: block_extra_info(block_extra_info_)
	{
		children.push_back(input_);
	}

	BlockExtraInfo getBlockExtraInfo() const override
	{
		return block_extra_info;
	}

	String getName() const override { return "BlockExtraInfoInput"; }

	String getID() const override
	{
		std::stringstream res;
		res << "BlockExtraInfoInput(" << children.back()->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl() override
	{
		return children.back()->read();
	}

private:
	BlockExtraInfo block_extra_info;
};

}
