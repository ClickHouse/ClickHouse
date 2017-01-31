#pragma once

#include <DB/Core/Progress.h>

#include <DB/Interpreters/Limits.h>

#include <DB/DataStreams/BlockStreamProfileInfo.h>
#include <DB/DataStreams/IBlockInputStream.h>

#include <atomic>

namespace DB
{

class QuotaForIntervals;
struct ProcessListElement;
class IProfilingBlockInputStream;

using ProfilingBlockInputStreamPtr = std::shared_ptr<IProfilingBlockInputStream>;


/** Смотрит за тем, как работает источник блоков.
  * Позволяет получить информацию для профайлинга:
  *  строк в секунду, блоков в секунду, мегабайт в секунду и т. п.
  * Позволяет остановить чтение данных (во вложенных источниках).
  */
class IProfilingBlockInputStream : public IBlockInputStream
{
public:
	Block read() override final;

	/** Реализация по-умолчанию вызывает readPrefixImpl() у себя, а затем readPrefix() у всех детей рекурсивно.
	  * Есть случаи, когда вы не хотите, чтобы readPrefix у детей вызывался синхронно, в этой функции,
	  *  а хотите, чтобы они вызывались, например, в отдельных потоках (для распараллеливания инициализации детей).
	  * Тогда перегрузите функцию readPrefix.
	  */
	void readPrefix() override;

	/** Реализация по-умолчанию вызывает рекурсивно readSuffix() у всех детей, а затем readSuffixImpl() у себя.
	  * Если этот поток вызывает у детей read() в отдельном потоке, этот поведение обычно неверно:
	  * readSuffix() у ребенка нельзя вызывать в момент, когда read() того же ребенка выполняется в другом потоке.
	  * В таком случае нужно переопределить этот метод, чтобы readSuffix() у детей вызывался, например, после соединения потоков.
	  */
	void readSuffix() override;

	/// Get information about execution speed.
	const BlockStreamProfileInfo & getProfileInfo() const { return info; }

	/** Получить "тотальные" значения.
	  * Реализация по-умолчанию берёт их из себя или из первого дочернего источника, в котором они есть.
	  * Переопределённый метод может провести некоторые вычисления. Например, применить выражение к totals дочернего источника.
	  * Тотальных значений может не быть - тогда возвращается пустой блок.
	  *
	  * Вызывайте этот метод только после получения всех данных с помощью read,
	  *  иначе будут проблемы, если какие-то данные в это же время вычисляются в другом потоке.
	  */
	virtual const Block & getTotals();

	/// То же самое для минимумов и максимумов.
	const Block & getExtremes() const;


	/** Установить колбэк прогресса выполнения.
	  * Колбэк пробрасывается во все дочерние источники.
	  * По-умолчанию, он вызывается для листовых источников, после каждого блока.
	  * (Но это может быть переопределено в методе progress())
	  * Функция принимает количество строк в последнем блоке, количество байт в последнем блоке.
	  * Следует иметь ввиду, что колбэк может вызываться из разных потоков.
	  */
	void setProgressCallback(ProgressCallback callback);


	/** В этом методе:
	  * - вызывается колбэк прогресса;
	  * - обновляется статус выполнения запроса в ProcessList-е;
	  * - проверяются ограничения и квоты, которые должны быть проверены не в рамках одного источника,
	  *   а над общим количеством потраченных ресурсов во всех источниках сразу (информация в ProcessList-е).
	  */
	virtual void progress(const Progress & value)
	{
		/// Данные для прогресса берутся из листовых источников.
		if (children.empty())
			progressImpl(value);
	}

	void progressImpl(const Progress & value);


	/** Установить указатель на элемент списка процессов.
	  * Пробрасывается во все дочерние источники.
	  * В него будет записываться общая информация о потраченных на запрос ресурсах.
	  * На основе этой информации будет проверяться квота, и некоторые ограничения.
	  * Также эта информация будет доступна в запросе SHOW PROCESSLIST.
	  */
	void setProcessListElement(ProcessListElement * elem);

	/** Установить информацию о приблизительном общем количестве строк, которых нужно прочитать.
	  */
	void setTotalRowsApprox(size_t value) { total_rows_approx = value; }


	/** Попросить прервать получение данных как можно скорее.
	  * По-умолчанию - просто выставляет флаг is_cancelled и просит прерваться всех детей.
	  * Эта функция может вызываться несколько раз, в том числе, одновременно из разных потоков.
	  */
	virtual void cancel();

	/** Требуется ли прервать получение данных.
	 */
	bool isCancelled() const
	{
		return is_cancelled.load(std::memory_order_seq_cst);
	}

	/** What limitations and quotas should be checked.
	  * LIMITS_CURRENT - checks amount of data read by current stream only (BlockStreamProfileInfo is used for check).
	  *  Currently it is used in root streams to check max_result_{rows,bytes} limits.
	  * LIMITS_TOTAL - checks total amount of read data from leaf streams (i.e. data read from disk and remote servers).
	  *  It is checks max_{rows,bytes}_to_read in progress handler and use info from ProcessListElement::progress_in for this.
	  *  Currently this check is performed only in leaf streams.
	  */
	enum LimitsMode
	{
		LIMITS_CURRENT,
		LIMITS_TOTAL,
	};

	/// It is a subset of limitations from Limits.
	struct LocalLimits
	{
		LimitsMode mode = LIMITS_CURRENT;

		/// If it is zero, corresponding limit check isn't performed.
		size_t max_rows_to_read = 0;
		size_t max_bytes_to_read = 0;
		OverflowMode read_overflow_mode = OverflowMode::THROW;

		Poco::Timespan max_execution_time = 0;
		OverflowMode timeout_overflow_mode = OverflowMode::THROW;

		/// in rows per second
		size_t min_execution_speed = 0;
		/// Проверять, что скорость не слишком низкая, после прошествия указанного времени.
		Poco::Timespan timeout_before_checking_execution_speed = 0;
	};

	/** Set limitations that checked on each block. */
	void setLimits(const LocalLimits & limits_)
	{
		limits = limits_;
	}

	const LocalLimits & getLimits() const
	{
		return limits;
	}

	/** Установить квоту. Если устанавливается квота на объём исходных данных,
	  * то следует ещё установить mode = LIMITS_TOTAL в LocalLimits с помощью setLimits.
	  */
	void setQuota(QuotaForIntervals & quota_)
	{
		quota = &quota_;
	}

	/// Включить рассчёт минимумов и максимумов по столбцам результата.
	void enableExtremes() { enabled_extremes = true; }

protected:
	BlockStreamProfileInfo info;
	std::atomic<bool> is_cancelled{false};
	ProgressCallback progress_callback;
	ProcessListElement * process_list_elem = nullptr;

	bool enabled_extremes = false;

	/// Дополнительная информация, которая может образоваться в процессе работы.

	/// Тотальные значения при агрегации.
	Block totals;
	/// Минимумы и максимумы. Первая строчка блока - минимумы, вторая - максимумы.
	Block extremes;
	/// Приблизительное общее количество строк, которых нужно прочитать. Для прогресс-бара.
	size_t total_rows_approx = 0;
	/// Информация о приблизительном общем количестве строк собрана в родительском источнике.
	bool collected_total_rows_approx = false;

	/// Превышено ограничение на количество строк/байт, и нужно прекратить выполнение на следующем вызове read, как будто поток иссяк.
	bool limit_exceeded_need_break = false;

	/// Ограничения и квоты.

	LocalLimits limits;

	QuotaForIntervals * quota = nullptr;	/// Если nullptr - квота не используется.
	double prev_elapsed = 0;

	/// Наследники должны реализовать эту функцию.
	virtual Block readImpl() = 0;

	/// Здесь можно делать предварительную инициализацию.
	virtual void readPrefixImpl() {}

	/// Здесь необходимо делать финализацию, которая может привести к исключению.
	virtual void readSuffixImpl() {}

	void updateExtremes(Block & block);

	/** Проверить ограничения и квоты.
	  * Но только те, что могут быть проверены в рамках каждого отдельного источника.
	  */
	bool checkLimits();
	void checkQuota(Block & block);

	/// Собрать информацию о приблизительном общем числе строк по всем детям.
	void collectTotalRowsApprox();

	/** Передать информацию о приблизительном общем числе строк в колбэк прогресса.
	  * Сделано так, что отправка происходит лишь в верхнем источнике.
	  */
	void collectAndSendTotalRowsApprox();
};

}
