#include <DB/Core/Block.h>


namespace DB
{

class SquashingTransform
{
public:
	SquashingTransform(size_t min_block_size_rows, size_t min_block_size_bytes);

	struct Result
	{
		bool ready = false;
		Block block;

		Result(bool ready_) : ready(ready_) {}
		Result(Block && block_) : ready(true), block(std::move(block_)) {}
	};

	Result add(Block && block);

private:
	size_t min_block_size_rows;
	size_t min_block_size_bytes;

	Block accumulated_block;
	bool all_read = false;

	void append(Block && block);

	bool isEnoughSize(size_t rows, size_t bytes) const;
};

}
