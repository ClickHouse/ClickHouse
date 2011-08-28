#pragma once

#include <Poco/SharedPtr.h>
#include <Poco/Stopwatch.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/// Информация для профайлинга.
struct BlockStreamProfileInfo
{
	bool started;
	Poco::Stopwatch work_stopwatch;		/// Время вычислений (выполнения функции read())
	Poco::Stopwatch total_stopwatch;	/// Время с учётом ожидания

	size_t rows;
	size_t blocks;
	size_t bytes;

	BlockStreamProfileInfo() : started(false), rows(0), blocks(0), bytes(0) {}

	void update(Block & block);
	void print(std::ostream & ostr) const;
};

	
/** Смотрит за тем, как работает другой поток блоков.
  * Позволяет получить информацию для профайлинга:
  *  строк в секунду, блоков в секунду, мегабайт в секунду и т. п.
  */
class ProfilingBlockInputStream : public IBlockInputStream
{
public:
	ProfilingBlockInputStream(BlockInputStreamPtr in_)
		: in(in_) {}
	
	Block read();
	
	const BlockStreamProfileInfo & getInfo() const;

private:
	BlockInputStreamPtr in;
	BlockStreamProfileInfo info;
};

}
