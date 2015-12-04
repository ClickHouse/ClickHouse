#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Поток блоков, из которого можно прочитать следующий блок из явно предоставленного списка.
  * Также смотрите OneBlockInputStream.
  */
class BlocksListBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Захватывает владение списком блоков.
	BlocksListBlockInputStream(BlocksList && list_)
		: list(std::move(list_)), it(list.begin()), end(list.end()) {}

	/// Использует лежащий где-то ещё список блоков.
	BlocksListBlockInputStream(BlocksList::iterator & begin_, BlocksList::iterator & end_)
		: it(begin_), end(end_) {}

	String getName() const override { return "BlocksList"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

protected:
	Block readImpl() override
	{
		if (it == end)
			return Block();

		Block res = *it;
		++it;
		return res;
	}

private:
	BlocksList list;
	BlocksList::iterator it;
	const BlocksList::iterator end;
};

}
