#pragma once

#include <list>
#include <stack>
#include <atomic>
#include <thread>
#include <mutex>

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


/** Позволяет обработать множество источников блоков параллельно, используя указанное количество потоков.
  * Вынимает из любого доступного источника блок и передаёт его на обработку в предоставленный handler.
  *
  * Устроено так:
  * - есть набор источников, из которых можно вынимать блоки;
  * - есть набор потоков, которые могут одновременно вынимать блоки из разных источников;
  * - "свободные" источники (с которыми сейчас не работает никакой поток) кладутся в очередь источников;
  * - когда поток берёт источник для обработки, он удаляет его из очереди источников,
  *    вынимает из него блок, и затем кладёт источник обратно в очередь источников;
  */

namespace DB
{


/// Пример обработчика.
struct ParallelInputsHandler
{
	/// Обработка блока данных.
	void onBlock(Block & block, size_t thread_num) {}

	/// Блоки закончились. Из-за того, что все источники иссякли или из-за отмены работы.
	/// Этот метод всегда вызывается ровно один раз, в конце работы, если метод onException не кидает исключение.
	void onFinish() {}

	/// Обработка исключения. Разумно вызывать в этом методе метод ParallelInputsProcessor::cancel, а также передавать эксепшен в основной поток.
	void onException(ExceptionPtr & exception, size_t thread_num) {}
};


template <typename Handler>
class ParallelInputsProcessor
{
public:
	ParallelInputsProcessor(BlockInputStreams inputs_, size_t max_threads_, Handler & handler_)
		: inputs(inputs_), max_threads(std::min(inputs_.size(), max_threads_)), handler(handler_)
	{
		for (size_t i = 0; i < inputs_.size(); ++i)
			input_stack.emplace(inputs_[i], i);
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


	void thread(MemoryTracker * memory_tracker, size_t thread_num)
	{
		current_memory_tracker = memory_tracker;
		ExceptionPtr exception;

		try
		{
			loop(thread_num);
		}
		catch (...)
		{
			exception = cloneCurrentException();
		}

		if (exception)
		{
			handler.onException(exception, thread_num);
		}

		/// Последний поток при выходе сообщает, что данных больше нет.
		if (0 == --active_threads)
		{
			handler.onFinish();
		}
	}

	void loop(size_t thread_num)
	{
		while (!finish)	/// Может потребоваться прекратить работу раньше, чем все источники иссякнут.
		{
			InputData input;

			/// Выбираем следующий источник.
			{
				std::lock_guard<std::mutex> lock(input_stack_mutex);

				/// Если свободных источников нет, то этот поток больше не нужен. (Но другие потоки могут работать со своими источниками.)
				if (input_stack.empty())
					break;

				input = input_stack.top();

				/// Убираем источник из очереди доступных источников.
				input_stack.pop();
			}

			/// Основная работа.
			Block block = input.in->read();

			{
				if (finish)
					break;

				/// Если этот источник ещё не иссяк, то положим полученный блок в очередь готовых.
				{
					std::lock_guard<std::mutex> lock(input_stack_mutex);

					if (block)
					{
						input_stack.push(input);
					}
					else
					{
						if (input_stack.empty())
							break;
					}
				}

				if (finish)
					break;

				if (block)
					handler.onBlock(block, thread_num);
			}
		}
	}

	BlockInputStreams inputs;
	unsigned max_threads;

	Handler & handler;

	/// Потоки.
	typedef std::vector<std::thread> ThreadsData;
	ThreadsData threads;

	/** Стек доступных источников, которые не заняты каким-либо потоком в данный момент.
	  * Стек вместо очереди - чтобы выполнять работу по чтению одного источника более последовательно.
	  * То есть, продолжать обработку источника, который недавно обрабатывался.
	  */
	typedef std::stack<InputData> InputStack;
	InputStack input_stack;

	/// Для операций с input_stack.
	std::mutex input_stack_mutex;

	/// Сколько источников иссякло.
	std::atomic<size_t> active_threads { 0 };
	/// Завершить работу потоков (раньше, чем иссякнут источники).
	std::atomic<bool> finish { false };
	/// Подождали завершения всех потоков.
	std::atomic<bool> joined_threads { false };

	Logger * log = &Logger::get("ParallelInputsProcessor");
};


}
