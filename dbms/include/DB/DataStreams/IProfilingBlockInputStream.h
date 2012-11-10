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
	IProfilingBlockInputStream() : is_cancelled(false) {}
	
	Block read();

	/// Получить информацию о скорости выполнения.
	const BlockStreamProfileInfo & getInfo() const;


	/** Установить колбэк прогресса выполнения.
	  * Колбэк пробрасывается во все источники.
	  * По-умолчанию, он вызывается для листовых источников, после каждого блока.
	  * (Но это может быть переопределено в методе progress())
	  * Функция принимает количество строк в последнем блоке, количество байт в последнем блоке.
	  * Следует иметь ввиду, что колбэк может вызываться из разных потоков.
	  */
	typedef boost::function<void(size_t, size_t)> ProgressCallback;
	void setProgressCallback(ProgressCallback callback);

	virtual void progress(Block & block);


	/** Попросить прервать получение данных как можно скорее.
	  * По-умолчанию - просто выставляет флаг is_cancelled и просит прерваться всех детей.
	  * Эта функция может вызываться несколько раз, в том числе, одновременно из разных потоков.
	  */
	virtual void cancel();

	/** Требуется ли прервать получение данных.
	 */
	bool isCancelled()
	{
		return is_cancelled;
	}

protected:
	BlockStreamProfileInfo info;
	volatile bool is_cancelled;
	ProgressCallback progress_callback;

	/// Наследники должны реализовать эту функцию.
	virtual Block readImpl() = 0;
};

}
