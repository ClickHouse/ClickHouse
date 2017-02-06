#pragma once

#include <list>
#include <queue>
#include <atomic>
#include <thread>
#include <mutex>

#include <common/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>


/** Allows to process multiple block input streams (sources) in parallel, using specified number of threads.
  * Reads (pulls) blocks from any available source and passes it to specified handler.
  *
  * Implemented in following way:
  * - there are multiple input sources to read blocks from;
  * - there are multiple threads, that could simultaneously read blocks from different sources;
  * - "available" sources (that are not read in any thread right now) are put in queue of sources;
  * - when thread take a source to read from, it removes source from queue of sources,
  *    then read block from source and then put source back to queue of available sources.
  */

namespace CurrentMetrics
{
	extern const Metric QueryThread;
}

namespace DB
{

/** Режим объединения.
  */
enum class StreamUnionMode
{
	Basic = 0, /// вынимать блоки
	ExtraInfo  /// вынимать блоки + дополнительную информацию
};

/// Пример обработчика.
struct ParallelInputsHandler
{
	/// Обработка блока данных.
	void onBlock(Block & block, size_t thread_num) {}

	/// Обработка блока данных + дополнительных информаций.
	void onBlock(Block & block, BlockExtraInfo & extra_info, size_t thread_num) {}

	/// Вызывается для каждого потока, когда потоку стало больше нечего делать.
	/// Из-за того, что иссякла часть источников, и сейчас источников осталось меньше, чем потоков.
	/// Вызывается, если метод onException не кидает исключение; вызывается до метода onFinish.
	void onFinishThread(size_t thread_num) {}

	/// Блоки закончились. Из-за того, что все источники иссякли или из-за отмены работы.
	/// Этот метод всегда вызывается ровно один раз, в конце работы, если метод onException не кидает исключение.
	void onFinish() {}

	/// Обработка исключения. Разумно вызывать в этом методе метод ParallelInputsProcessor::cancel, а также передавать эксепшен в основной поток.
	void onException(std::exception_ptr & exception, size_t thread_num) {}
};


template <typename Handler, StreamUnionMode mode = StreamUnionMode::Basic>
class ParallelInputsProcessor
{
public:
	/** additional_input_at_end - если не nullptr,
	  *  то из этого источника начинают доставаться блоки лишь после того, как все остальные источники обработаны.
	  * Это делается в основном потоке.
	  *
	  * Предназначено для реализации FULL и RIGHT JOIN
	  * - где нужно сначала параллельно сделать JOIN, при этом отмечая, какие ключи не найдены,
	  *   и только после завершения этой работы, создать блоки из ненайденных ключей.
	  */
	ParallelInputsProcessor(BlockInputStreams inputs_, BlockInputStreamPtr additional_input_at_end_, size_t max_threads_, Handler & handler_)
		: inputs(inputs_), additional_input_at_end(additional_input_at_end_), max_threads(std::min(inputs_.size(), max_threads_)), handler(handler_)
	{
		for (size_t i = 0; i < inputs_.size(); ++i)
			available_inputs.emplace(inputs_[i], i);
	}

	~ParallelInputsProcessor()
	{
		try
		{
			wait();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

	/// Запустить фоновые потоки, начать работу.
	void process()
	{
		active_threads = max_threads;
		threads.reserve(max_threads);
		for (size_t i = 0; i < max_threads; ++i)
			threads.emplace_back(std::bind(&ParallelInputsProcessor::thread, this, current_memory_tracker, i));
	}

	/// Попросить все источники остановиться раньше, чем они иссякнут.
	void cancel()
	{
		finish = true;

		for (auto & input : inputs)
		{
			if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*input))
			{
				try
				{
					child->cancel();
				}
				catch (...)
				{
					/** Если не удалось попросить остановиться одного или несколько источников.
					  * (например, разорвано соединение при распределённой обработке запроса)
					  * - то пофиг.
					  */
					LOG_ERROR(log, "Exception while cancelling " << child->getName());
				}
			}
		}
	}

	/// Подождать завершения работы всех потоков раньше деструктора.
	void wait()
	{
		if (joined_threads)
			return;

		for (auto & thread : threads)
			thread.join();

		threads.clear();
		joined_threads = true;
	}

	size_t getNumActiveThreads() const
	{
		return active_threads;
	}

private:
	/// Данные отдельного источника
	struct InputData
	{
		BlockInputStreamPtr in;
		size_t i;		/// Порядковый номер источника (для отладки).

		InputData() {}
		InputData(BlockInputStreamPtr & in_, size_t i_) : in(in_), i(i_) {}
	};

	template <StreamUnionMode mode2 = mode>
	void publishPayload(BlockInputStreamPtr & stream, Block & block, size_t thread_num,
		typename std::enable_if<mode2 == StreamUnionMode::Basic>::type * = nullptr)
	{
		handler.onBlock(block, thread_num);
	}

	template <StreamUnionMode mode2 = mode>
	void publishPayload(BlockInputStreamPtr & stream, Block & block, size_t thread_num,
		typename std::enable_if<mode2 == StreamUnionMode::ExtraInfo>::type * = nullptr)
	{
		BlockExtraInfo extra_info = stream->getBlockExtraInfo();
		handler.onBlock(block, extra_info, thread_num);
	}

	void thread(MemoryTracker * memory_tracker, size_t thread_num)
	{
		current_memory_tracker = memory_tracker;
		std::exception_ptr exception;

		setThreadName("ParalInputsProc");
		CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

		try
		{
			loop(thread_num);
		}
		catch (...)
		{
			exception = std::current_exception();
		}

		if (exception)
		{
			handler.onException(exception, thread_num);
		}

		handler.onFinishThread(thread_num);

		/// Последний поток при выходе сообщает, что данных больше нет.
		if (0 == --active_threads)
		{
			/// И ещё обрабатывает дополнительный источник, если такой есть.
			if (additional_input_at_end)
			{
				try
				{
					while (Block block = additional_input_at_end->read())
						publishPayload(additional_input_at_end, block, thread_num);
				}
				catch (...)
				{
					exception = std::current_exception();
				}

				if (exception)
				{
					handler.onException(exception, thread_num);
				}
			}

			handler.onFinish();		/// TODO Если в onFinish или onFinishThread эксепшен, то вызывается std::terminate.
		}
	}

	void loop(size_t thread_num)
	{
		while (!finish)	/// Может потребоваться прекратить работу раньше, чем все источники иссякнут.
		{
			InputData input;

			/// Выбираем следующий источник.
			{
				std::lock_guard<std::mutex> lock(available_inputs_mutex);

				/// Если свободных источников нет, то этот поток больше не нужен. (Но другие потоки могут работать со своими источниками.)
				if (available_inputs.empty())
					break;

				input = available_inputs.front();

				/// Убираем источник из очереди доступных источников.
				available_inputs.pop();
			}

			/// Основная работа.
			Block block = input.in->read();

			{
				if (finish)
					break;

				/// Если этот источник ещё не иссяк, то положим полученный блок в очередь готовых.
				{
					std::lock_guard<std::mutex> lock(available_inputs_mutex);

					if (block)
					{
						available_inputs.push(input);
					}
					else
					{
						if (available_inputs.empty())
							break;
					}
				}

				if (finish)
					break;

				if (block)
					publishPayload(input.in, block, thread_num);
			}
		}
	}

	BlockInputStreams inputs;
	BlockInputStreamPtr additional_input_at_end;
	unsigned max_threads;

	Handler & handler;

	/// Потоки.
	using ThreadsData = std::vector<std::thread>;
	ThreadsData threads;

	/** Набор доступных источников, которые не заняты каким-либо потоком в данный момент.
	  * Каждый поток берёт из этого набора один источник, вынимает из источника блок (в этот момент источник делает вычисления),
	  *  и (если источник не исчерпан), кладёт назад в набор доступных источников.
	  *
	  * Возникает вопрос, что лучше использовать:
	  * - очередь (только что обработанный источник будет в следующий раз обработан позже остальных)
	  * - стек (только что обработанный источник будет обработан как можно раньше).
	  *
	  * Стек лучше очереди, когда надо выполнять работу по чтению одного источника более последовательно,
	  *  и теоретически, это позволяет достичь более последовательных чтений с диска.
	  *
	  * Но при использовании стека, возникает проблема при распределённой обработке запроса:
	  *  данные всё-время читаются только с части серверов, а на остальных серверах
	  *  возникает таймаут при send-е, и обработка запроса завершается с исключением.
	  *
	  * Поэтому, используется очередь. Это можно улучшить в дальнейшем.
	  */
	using AvailableInputs = std::queue<InputData>;
	AvailableInputs available_inputs;

	/// Для операций с available_inputs.
	std::mutex available_inputs_mutex;

	/// Сколько источников иссякло.
	std::atomic<size_t> active_threads { 0 };
	/// Завершить работу потоков (раньше, чем иссякнут источники).
	std::atomic<bool> finish { false };
	/// Подождали завершения всех потоков.
	std::atomic<bool> joined_threads { false };

	Logger * log = &Logger::get("ParallelInputsProcessor");
};


}
