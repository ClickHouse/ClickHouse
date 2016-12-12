#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/ParallelInputsProcessor.h>


namespace DB
{


/** Агрегирует несколько источников параллельно.
  * Производит агрегацию блоков из разных источников независимо в разных потоках, затем объединяет результаты.
  * Если final == false, агрегатные функции не финализируются, то есть, не заменяются на своё значение, а содержат промежуточное состояние вычислений.
  * Это необходимо, чтобы можно было продолжить агрегацию (например, объединяя потоки частично агрегированных данных).
  */
class ParallelAggregatingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/** Столбцы из key_names и аргументы агрегатных функций, уже должны быть вычислены.
	  */
	ParallelAggregatingBlockInputStream(
		BlockInputStreams inputs, BlockInputStreamPtr additional_input_at_end,
		const Aggregator::Params & params_, bool final_, size_t max_threads_, size_t temporary_data_merge_threads_);

	String getName() const override { return "ParallelAggregating"; }

	String getID() const override;

	void cancel() override;

protected:
	/// Ничего не делаем, чтобы подготовка к выполнению запроса делалась параллельно, в ParallelInputsProcessor.
	void readPrefix() override
	{
	}

	Block readImpl() override;

private:
	Aggregator::Params params;
	Aggregator aggregator;
	bool final;
	size_t max_threads;
	size_t temporary_data_merge_threads;

	size_t keys_size;
	size_t aggregates_size;

	/** Используется, если есть ограничение на максимальное количество строк при агрегации,
	  *  и если group_by_overflow_mode == ANY.
	  * В этом случае, новые ключи не добавляются в набор, а производится агрегация только по
	  *  ключам, которые уже успели попасть в набор.
	  */
	bool no_more_keys = false;

	std::atomic<bool> executed {false};

	/// Для чтения сброшенных во временный файл данных.
	struct TemporaryFileStream
	{
		ReadBufferFromFile file_in;
		CompressedReadBuffer compressed_in;
		BlockInputStreamPtr block_in;

		TemporaryFileStream(const std::string & path);
	};
	std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

	Logger * log = &Logger::get("ParallelAggregatingBlockInputStream");


	ManyAggregatedDataVariants many_data;
	Exceptions exceptions;

	struct ThreadData
	{
		size_t src_rows = 0;
		size_t src_bytes = 0;

		StringRefs key;
		ConstColumnPlainPtrs key_columns;
		Aggregator::AggregateColumns aggregate_columns;
		Sizes key_sizes;

		ThreadData(size_t keys_size, size_t aggregates_size)
		{
			key.resize(keys_size);
			key_columns.resize(keys_size);
			aggregate_columns.resize(aggregates_size);
			key_sizes.resize(keys_size);
		}
	};

	std::vector<ThreadData> threads_data;


	struct Handler
	{
		Handler(ParallelAggregatingBlockInputStream & parent_)
			: parent(parent_) {}

		void onBlock(Block & block, size_t thread_num);
		void onFinishThread(size_t thread_num);
		void onFinish();
		void onException(std::exception_ptr & exception, size_t thread_num);

		ParallelAggregatingBlockInputStream & parent;
	};

	Handler handler;
	ParallelInputsProcessor<Handler> processor;


	void execute();


	/** Отсюда будем доставать готовые блоки после агрегации.
	  */
	std::unique_ptr<IBlockInputStream> impl;
};

}
