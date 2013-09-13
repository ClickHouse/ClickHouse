#pragma once

#include <boost/function.hpp>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Names.h>

#include <DB/Interpreters/Limits.h>
#include <DB/Interpreters/Quota.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


/// Информация для профайлинга.
struct BlockStreamProfileInfo
{
	bool started;
	Stopwatch work_stopwatch;	/// Время вычислений (выполнения функции read())
	Stopwatch total_stopwatch;	/// Время с учётом ожидания
	
	String stream_name;			/// Короткое имя потока, для которого собирается информация

	size_t rows;
	size_t blocks;
	size_t bytes;

	/// Информация о вложенных потоках - для выделения чистого времени работы.
	typedef std::vector<const BlockStreamProfileInfo *> BlockStreamProfileInfos;
	BlockStreamProfileInfos nested_infos;

	String column_names;

	BlockStreamProfileInfo() :
		started(false), rows(0), blocks(0), bytes(0),
		applied_limit(false), rows_before_limit(0), calculated_rows_before_limit(false)
	{
	}
	
	size_t getRowsBeforeLimit() const;
	bool hasAppliedLimit() const;

	void update(Block & block);
	void print(std::ostream & ostr) const;	
	
	/// Методы для бинарной [де]сериализации
	void read(ReadBuffer & in);
	void write(WriteBuffer & out) const;
	
private:
	void calculateRowsBeforeLimit() const;
	
	/// Для этих полей сделаем accessor'ы, т.к. их необходимо предварительно вычислять.
	mutable bool applied_limit;					/// Применялся ли LIMIT
	mutable size_t rows_before_limit;			/// Число строк до выполнения LIMIT
	mutable bool calculated_rows_before_limit;	/// Вычислялось ли поле rows_before_limit
};

	
/** Смотрит за тем, как работает источник блоков.
  * Позволяет получить информацию для профайлинга:
  *  строк в секунду, блоков в секунду, мегабайт в секунду и т. п.
  * Позволяет остановить чтение данных (во вложенных источниках).
  */
class IProfilingBlockInputStream : public IBlockInputStream
{
public:
	IProfilingBlockInputStream(StoragePtr owned_storage_ = StoragePtr())
		: IBlockInputStream(owned_storage_), is_cancelled(false), enabled_extremes(false), quota(NULL), quota_mode(QUOTA_READ), prev_elapsed(0) {}
	
	Block read();

	/// Реализация по-умолчанию вызывает рекурсивно readSuffix() у всех детей, а затем readSuffixImpl() у себя.
	void readSuffix();

	/// Получить информацию о скорости выполнения.
	const BlockStreamProfileInfo & getInfo() const;

	/** Получить "тотальные" значения.
	  * Реализация по-умолчанию берёт их из себя или из первого дочернего источника, в котором они есть.
	  * Переопределённый метод может провести некоторые вычисления. Например, применить выражение к totals дочернего источника.
	  * Тотальных значений может не быть - тогда возвращается пустой блок.
	  */
	virtual const Block & getTotals();
	
	/// То же самое для минимумов и максимумов.
	const Block & getExtremes() const;


	/** Установить колбэк прогресса выполнения.
	  * Колбэк пробрасывается во все источники.
	  * По-умолчанию, он вызывается для листовых источников, после каждого блока.
	  * (Но это может быть переопределено в методе progress())
	  * Функция принимает количество строк в последнем блоке, количество байт в последнем блоке.
	  * Следует иметь ввиду, что колбэк может вызываться из разных потоков.
	  */
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

	/// Используется подмножество ограничений из Limits.
	struct LocalLimits
	{
		size_t max_rows_to_read;
		size_t max_bytes_to_read;
		Limits::OverflowMode read_overflow_mode;

		Poco::Timespan max_execution_time;
		Limits::OverflowMode timeout_overflow_mode;

		/// В строчках в секунду.
		size_t min_execution_speed;
		/// Проверять, что скорость не слишком низкая, после прошествия указанного времени.
		Poco::Timespan timeout_before_checking_execution_speed;

		LocalLimits()
			: max_rows_to_read(0), max_bytes_to_read(0), read_overflow_mode(Limits::THROW),
			max_execution_time(0), timeout_overflow_mode(Limits::THROW),
			min_execution_speed(0), timeout_before_checking_execution_speed(0)
		{
		}
	};

	/** Установить ограничения для проверки на каждый блок. */
	void setLimits(const LocalLimits & limits_)
	{
		limits = limits_;
	}


	/// Какая квота используется - на объём исходных данных или на объём результата.
	enum QuotaMode
	{
		QUOTA_READ,
		QUOTA_RESULT,
	};

	/// Установить квоту.
	void setQuota(QuotaForIntervals & quota_, QuotaMode quota_mode_)
	{
		quota = &quota_;
		quota_mode = quota_mode_;
	}

	/// Включить рассчёт минимумов и максимумов по столбцам результата.
	void enableExtremes() { enabled_extremes = true; }

protected:
	BlockStreamProfileInfo info;
	volatile bool is_cancelled;
	ProgressCallback progress_callback;

	bool enabled_extremes;

	/// Дополнительная информация, которая может образоваться в процессе работы.

	/// Тотальные значения при агрегации.
	Block totals;
	/// Минимумы и максимумы. Первая строчка блока - минимумы, вторая - максимумы.
	Block extremes;

	/// Ограничения и квоты.
	
	LocalLimits limits;

	QuotaForIntervals * quota;	/// Если NULL - квота не используется.
	QuotaMode quota_mode;
	double prev_elapsed;

	/// Наследники должны реализовать эту функцию.
	virtual Block readImpl() = 0;

	/// Здесь необходимо делать финализацию, которая может привести к исключению.
	virtual void readSuffixImpl() {}

	void updateExtremes(Block & block);
	bool checkLimits();
	void checkQuota(Block & block);
};

}
