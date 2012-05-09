#pragma once

#include <boost/function.hpp>

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

	/// Информация о вложенных потоках - для выделения чистого времени работы.
	typedef std::vector<const BlockStreamProfileInfo *> BlockStreamProfileInfos;
	BlockStreamProfileInfos nested_infos;

	String column_names;

	BlockStreamProfileInfo() : started(false), rows(0), blocks(0), bytes(0) {}

	void update(Block & block);
	void print(std::ostream & ostr) const;
};

	
/** Смотрит за тем, как работает источник блоков.
  * Позволяет получить информацию для профайлинга:
  *  строк в секунду, блоков в секунду, мегабайт в секунду и т. п.
  * Позволяет остановить чтение данных (во вложенных источниках).
  */
class IProfilingBlockInputStream : public IBlockInputStream
{
public:
	Block read();

	/// Наследники должны реализовать эту функцию.
	virtual Block readImpl() = 0;
	
	const BlockStreamProfileInfo & getInfo() const;

	/** Установить колбэк, который вызывается, чтобы проверить, не был ли запрос остановлен.
	  * Колбэк пробрасывается во все листовые источники и вызывается там перед чтением данных.
	  * Следует иметь ввиду, что колбэк может вызываться из разных потоков.
	  */
	typedef boost::function<bool()> IsCancelledCallback;
	void setIsCancelledCallback(IsCancelledCallback callback);

private:
	BlockStreamProfileInfo info;
	IsCancelledCallback is_cancelled_callback;
};

}
