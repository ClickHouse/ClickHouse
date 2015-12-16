#pragma once

#include <DB/Interpreters/Aggregator.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/BlocksListBlockInputStream.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DB/DataStreams/ParallelInputsProcessor.h>
#include <common/Revision.h>


namespace DB
{

using Poco::SharedPtr;


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
		const Aggregator::Params & params_, bool final_, size_t max_threads_, size_t temporary_data_merge_threads_)
		: params(params_), aggregator(params),
		final(final_), max_threads(std::min(inputs.size(), max_threads_)), temporary_data_merge_threads(temporary_data_merge_threads_),
		keys_size(params.keys_size), aggregates_size(params.aggregates_size),
		handler(*this), processor(inputs, additional_input_at_end, max_threads, handler)
	{
		children = inputs;
		if (additional_input_at_end)
			children.push_back(additional_input_at_end);
	}

	String getName() const override { return "ParallelAggregating"; }

	String getID() const override
	{
		std::stringstream res;
		res << "ParallelAggregating(";

		Strings children_ids(children.size());
		for (size_t i = 0; i < children.size(); ++i)
			children_ids[i] = children[i]->getID();

		/// Порядок не имеет значения.
		std::sort(children_ids.begin(), children_ids.end());

		for (size_t i = 0; i < children_ids.size(); ++i)
			res << (i == 0 ? "" : ", ") << children_ids[i];

		res << ", " << aggregator.getID() << ")";
		return res.str();
	}

	void cancel() override
	{
		bool old_val = false;
		if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
			return;

		if (!executed)
			processor.cancel();
	}

protected:
	Block readImpl() override
	{
		if (!executed)
		{
			Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
			aggregator.setCancellationHook(hook);

			execute();

			if (isCancelled())
				return {};

			if (!aggregator.hasTemporaryFiles())
			{
				/** Если все частично-агрегированные данные в оперативке, то мерджим их параллельно, тоже в оперативке.
				  */
				impl = aggregator.mergeAndConvertToBlocks(many_data, final, max_threads);
			}
			else
			{
				/** Если есть временные файлы с частично-агрегированными данными на диске,
				  *  то читаем и мерджим их, расходуя минимальное количество памяти.
				  */

				ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

				const auto & files = aggregator.getTemporaryFiles();
				BlockInputStreams input_streams;
				for (const auto & file : files.files)
				{
					temporary_inputs.emplace_back(new TemporaryFileStream(file->path()));
					input_streams.emplace_back(temporary_inputs.back()->block_in);
				}

				LOG_TRACE(log, "Will merge " << files.files.size() << " temporary files of size "
					<< (files.sum_size_compressed / 1048576.0) << " MiB compressed, "
					<< (files.sum_size_uncompressed / 1048576.0) << " MiB uncompressed.");

				impl.reset(new MergingAggregatedMemoryEfficientBlockInputStream(
					input_streams, params, final, temporary_data_merge_threads, temporary_data_merge_threads));
			}

			executed = true;
		}

		Block res;
		if (isCancelled() || !impl)
			return res;

		return impl->read();
	}

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

		TemporaryFileStream(const std::string & path)
			: file_in(path), compressed_in(file_in), block_in(new NativeBlockInputStream(compressed_in, Revision::get())) {}
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

		void onBlock(Block & block, size_t thread_num)
		{
			parent.aggregator.executeOnBlock(block, *parent.many_data[thread_num],
				parent.threads_data[thread_num].key_columns, parent.threads_data[thread_num].aggregate_columns,
				parent.threads_data[thread_num].key_sizes, parent.threads_data[thread_num].key,
				parent.no_more_keys);

			parent.threads_data[thread_num].src_rows += block.rowsInFirstColumn();
			parent.threads_data[thread_num].src_bytes += block.bytes();
		}

		void onFinishThread(size_t thread_num)
		{
			if (!parent.isCancelled() && parent.aggregator.hasTemporaryFiles())
			{
				/// Сбросим имеющиеся в оперативке данные тоже на диск. Так проще их потом объединять.
				auto & data = *parent.many_data[thread_num];

				if (data.isConvertibleToTwoLevel())
					data.convertToTwoLevel();

				size_t rows = data.sizeWithoutOverflowRow();
				if (rows)
					parent.aggregator.writeToTemporaryFile(data, rows);
			}
		}

		void onFinish()
		{
			if (!parent.isCancelled() && parent.aggregator.hasTemporaryFiles())
			{
				/// Может так получиться, что какие-то данные ещё не сброшены на диск,
				///  потому что во время вызова onFinishThread ещё никакие данные не были сброшены на диск, а потом какие-то - были.
				for (auto & data : parent.many_data)
				{
					if (data->isConvertibleToTwoLevel())
						data->convertToTwoLevel();

					size_t rows = data->sizeWithoutOverflowRow();
					if (rows)
						parent.aggregator.writeToTemporaryFile(*data, rows);
				}
			}
		}

		void onException(std::exception_ptr & exception, size_t thread_num)
		{
			parent.exceptions[thread_num] = exception;
			parent.cancel();
		}

		ParallelAggregatingBlockInputStream & parent;
	};

	Handler handler;
	ParallelInputsProcessor<Handler> processor;


	void execute()
	{
		many_data.resize(max_threads);
		exceptions.resize(max_threads);

		for (size_t i = 0; i < max_threads; ++i)
			threads_data.emplace_back(keys_size, aggregates_size);

		LOG_TRACE(log, "Aggregating");

		Stopwatch watch;

		for (auto & elem : many_data)
			elem = new AggregatedDataVariants;

		processor.process();
		processor.wait();

		rethrowFirstException(exceptions);

		if (isCancelled())
			return;

		double elapsed_seconds = watch.elapsedSeconds();

		size_t total_src_rows = 0;
		size_t total_src_bytes = 0;
		for (size_t i = 0; i < max_threads; ++i)
		{
			size_t rows = many_data[i]->size();
			LOG_TRACE(log, std::fixed << std::setprecision(3)
				<< "Aggregated. " << threads_data[i].src_rows << " to " << rows << " rows"
					<< " (from " << threads_data[i].src_bytes / 1048576.0 << " MiB)"
				<< " in " << elapsed_seconds << " sec."
				<< " (" << threads_data[i].src_rows / elapsed_seconds << " rows/sec., "
					<< threads_data[i].src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

			total_src_rows += threads_data[i].src_rows;
			total_src_bytes += threads_data[i].src_bytes;
		}
		LOG_TRACE(log, std::fixed << std::setprecision(3)
			<< "Total aggregated. " << total_src_rows << " rows (from " << total_src_bytes / 1048576.0 << " MiB)"
			<< " in " << elapsed_seconds << " sec."
			<< " (" << total_src_rows / elapsed_seconds << " rows/sec., " << total_src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");
	}


	/** Отсюда будем доставать готовые блоки после агрегации.
	  */
	std::unique_ptr<IBlockInputStream> impl;
};

}
