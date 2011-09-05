#pragma once

#include <Poco/Stopwatch.h>

#include <DB/Core/Names.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


/// Информация для профайлинга.
struct BlockStreamProfileInfo
{
	bool started;
	Poco::Stopwatch work_stopwatch;		/// Время вычислений (выполнения функции read())
	Poco::Stopwatch total_stopwatch;	/// Время с учётом ожидания

	size_t rows;
	size_t blocks;
	size_t bytes;

	String column_names;

	BlockStreamProfileInfo() : started(false), rows(0), blocks(0), bytes(0) {}

	void update(Block & block);
	void print(std::ostream & ostr) const;
};

	
/** Смотрит за тем, как работает поток блоков.
  * Позволяет получить информацию для профайлинга:
  *  строк в секунду, блоков в секунду, мегабайт в секунду и т. п.
  */
class IProfilingBlockInputStream : public IBlockInputStream
{
public:
	Block read();

	/// Наследники должны реализовать эту функцию.
	virtual Block readImpl() = 0;
	
	const BlockStreamProfileInfo & getInfo() const;

private:
	BlockStreamProfileInfo info;
};

}
