#pragma once

#include <statdaemons/threadpool.hpp>

#include <DB/Common/PODArray.h>
#include <DB/Interpreters/Aggregator.h>


namespace DB
{


/** Агрегирует источник блоков параллельно в нескольких потоках.
  * Распараллеливание производится для каждого блока.
  * Для этого, сначала параллельно вычисляет хэши от ключей всех строчек блока,
  *  затем агрегирует строчки с разными диапазонами хэшей в разные хэш-таблицы
  *  (для разделения по хэш-таблицам используются другие биты ключа или другие хэш функции, чем те, что внутри хэш-таблиц).
  * Получится, что эти хэш-таблицы будут содержать разные ключи, и их не потребуется объединять.
  *
  * Хорошо работает при большом размере результата агрегации (количестве уникальных ключей)
  *  - линейно масштабируется по количеству потоков.
  *
  * Не работает при числе потоков больше 256.
  * Плохо работает при размере хэш-таблиц больше 2^32 элементов.
  *
  * TODO:
  * - поддержка with_totals;
  * - проверить работу при распределённой обработке запроса;
  * - починить rows_before_limit_at_least;
  * - минимальное количество строк на один поток; если в блоке мало строк - читать и обрабатывать несколько блоков сразу;
  * - определиться, в каких случаях следует использовать этот агрегатор, а в каких - нет.
  */
class SplittingAggregator : private Aggregator
{
public:
	SplittingAggregator(const ColumnNumbers & keys_, const AggregateDescriptions & aggregates_, size_t threads_,
		bool with_totals_, size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: Aggregator(keys_, aggregates_, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_), threads(threads_), pool(threads),
		log(&Logger::get("SplittingAggregator")), method(AggregatedDataVariants::Type::EMPTY),
		key_columns(keys_size), aggregate_columns(aggregates_size), rows(0), src_rows(0), src_bytes(0), size_of_all_results(0)
	{
	}

	SplittingAggregator(const Names & key_names_, const AggregateDescriptions & aggregates_, size_t threads_,
		bool with_totals_, size_t max_rows_to_group_by_ = 0, OverflowMode group_by_overflow_mode_ = OverflowMode::THROW)
		: Aggregator(key_names_, aggregates_, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_), threads(threads_), pool(threads),
		log(&Logger::get("SplittingAggregator")), method(AggregatedDataVariants::Type::EMPTY),
		key_columns(keys_size), aggregate_columns(aggregates_size), rows(0), src_rows(0), src_bytes(0), size_of_all_results(0)
	{
	}

	/// Агрегировать источник. Получить результат в виде одной из структур данных.
	void execute(BlockInputStreamPtr stream, ManyAggregatedDataVariants & results);

	void convertToBlocks(ManyAggregatedDataVariants & data_variants, Blocks & blocks, bool final);

	String getID() const { return Aggregator::getID(); }

private:
	size_t threads;
	boost::threadpool::pool pool;

	/// Вычисленные значения ключей и хэшей хэш-таблицы.
	PODArray<UInt64> keys64;
	PODArray<UInt64> hashes64;
	PODArray<UInt128> keys128;
	PODArray<UInt128> hashes128;
	PODArray<StringRef> string_refs;

	PODArray<UInt8> thread_nums;

	Logger * log;

	/// Каким способом выполняется агрегация.
	AggregatedDataVariants::Type method;

	ConstColumnPlainPtrs key_columns;

	typedef std::vector<ConstColumnPlainPtrs> AggregateColumns;
	AggregateColumns aggregate_columns;

	size_t rows;

	size_t src_rows;
	size_t src_bytes;

	Sizes key_sizes;

	StringRefHash hash_func_string;

	/// Для более точного контроля max_rows_to_group_by.
	size_t size_of_all_results;

	void calculateHashesThread(Block & block, size_t begin, size_t end, ExceptionPtr & exception, MemoryTracker * memory_tracker);
	void aggregateThread(Block & block, AggregatedDataVariants & result, size_t thread_no, ExceptionPtr & exception, MemoryTracker * memory_tracker);
	void convertToBlockThread(AggregatedDataVariants & data_variant, Block & block, bool final, ExceptionPtr & exception, MemoryTracker * memory_tracker);

	template <typename FieldType>
	void aggregateOneNumber(AggregatedDataVariants & result, size_t thread_no, bool no_more_keys);
};


}
