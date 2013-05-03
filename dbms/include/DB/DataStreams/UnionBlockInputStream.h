#pragma once

#include <list>
#include <queue>

#include <Poco/Thread.h>

#include <Yandex/logger_useful.h>

#include <DB/Common/ConcurrentBoundedQueue.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Объединяет несколько источников в один.
  * Блоки из разных источников перемежаются друг с другом произвольным образом.
  * Можно указать количество потоков (max_threads),
  *  в которых будет выполняться получение данных из разных источников.
  *
  * Устроено так:
  * - есть набор источников, из которых можно вынимать блоки;
  * - есть набор потоков, которые могут одновременно вынимать блоки из разных источников;
  * - "свободные" источники (с которыми сейчас не работает никакой поток) кладутся в очередь источников;
  * - когда поток берёт источник для обработки, он удаляет его из очереди источников,
  *    вынимает из него блок, и затем кладёт источник обратно в очередь источников;
  * - полученные блоки складываются в ограниченную очередь готовых блоков;
  * - основной поток вынимает готовые блоки из очереди готовых блоков.
  */
class UnionBlockInputStream : public IProfilingBlockInputStream
{
	class Thread;
public:
	UnionBlockInputStream(BlockInputStreams inputs_, unsigned max_threads_ = 1)
		: max_threads(std::min(inputs_.size(), static_cast<size_t>(max_threads_))),
		output_queue(max_threads), exhausted_inputs(0), finish(false), all_read(false),
		log(&Logger::get("UnionBlockInputStream"))
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());

		for (size_t i = 0; i < inputs_.size(); ++i)
		{
			input_queue.push(InputData());
			input_queue.back().in = inputs_[i];
			input_queue.back().i = i;
		}
	}

	String getName() const { return "UnionBlockInputStream"; }

	BlockInputStreamPtr clone() { return new UnionBlockInputStream(children, max_threads); }

	~UnionBlockInputStream()
	{
		LOG_TRACE(log, "Waiting for threads to finish");

		finish = true;
		cancel();

		ExceptionPtr exception;

		/// Вынем всё, что есть в очереди готовых данных.
		OutputData res;
		while (output_queue.tryPop(res))
			if (res.exception && !exception)
				exception = res.exception;

		/** В этот момент, запоздавшие потоки ещё могут вставить в очередь какие-нибудь блоки, но очередь не переполнится.
		  * PS. Может быть, для переменной finish нужен барьер?
		  */

		for (ThreadsData::iterator it = threads_data.begin(); it != threads_data.end(); ++it)
			it->thread->join();

		/// Может быть, нам под конец положили эксепшен.
		while (output_queue.tryPop(res))
			if (res.exception && !exception)
				exception = res.exception;

		if (exception && !std::uncaught_exception())
			exception->rethrow();

		LOG_TRACE(log, "Waited for threads to finish");
	}

	/** Отличается от реализации по-умолчанию тем, что пытается остановить все источники,
	  *  пропуская отвалившиеся по эксепшену.
	  */
	void cancel()
	{
		if (!__sync_bool_compare_and_swap(&is_cancelled, false, true))
			return;

		ExceptionPtr exception;
		for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		{
			if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			{
				try
				{
					child->cancel();
				}
				catch (const Exception & e)
				{
					if (!exception)
						exception = e.clone();
				}
				catch (const Poco::Exception & e)
				{
					if (!exception)
						exception = e.clone();
				}
				catch (const std::exception & e)
				{
					if (!exception)
						exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
				}
				catch (...)
				{
					if (!exception)
						exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
				}
			}
		}

		if (exception)
			exception->rethrow();
	}

protected:
	Block readImpl()
	{
		OutputData res;
		if (all_read)
			return res.block;

		/// Запускаем потоки, если это ещё не было сделано.
		if (threads_data.empty())
		{
			threads_data.resize(max_threads);
			for (ThreadsData::iterator it = threads_data.begin(); it != threads_data.end(); ++it)
			{
				it->runnable = new Thread(*this);
				it->thread = new Poco::Thread;
				it->thread->start(*it->runnable);
			}
		}

		/// Будем ждать, пока будет готов следующий блок или будет выкинуто исключение.
		output_queue.pop(res);

		if (res.exception)
			res.exception->rethrow();

		if (!res.block)
			all_read = true;

		return res.block;
	}

private:
	/// Данные отдельного источника
	struct InputData
	{
		BlockInputStreamPtr in;
		size_t i;		/// Порядковый номер источника (для отладки).
	};


	/// Данные отдельного потока
	struct ThreadData
	{
		SharedPtr<Poco::Thread> thread;
		SharedPtr<Thread> runnable;
	};


	class Thread : public Poco::Runnable
	{
	public:
		Thread(UnionBlockInputStream & parent_) : parent(parent_) {}

		void run()
		{
			ExceptionPtr exception;
			
			try
			{
				loop();
			}
			catch (const Exception & e)
			{
				exception = e.clone();
			}
			catch (const Poco::Exception & e)
			{
				exception = e.clone();
			}
			catch (const std::exception & e)
			{
				exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
			}
			catch (...)
			{
				exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
			}

			if (exception)
			{
				/// Попросим остальные потоки побыстрее прекратить работу.
				parent.finish = true;

				try
				{
					parent.cancel();
				}
				catch (...)
				{
					/** Если не удалось попросить остановиться одного или несколько источников.
					  * (например, разорвано соединение при распределённой обработке запроса)
					  * - то пофиг.
					  */
				}

				/// Отдаём эксепшен в основной поток.
				parent.output_queue.push(exception);
			}
		}

		void loop()
		{
			while (!parent.finish)	/// Может потребоваться прекратить работу раньше, чем все источники иссякнут.
			{
				InputData input;

				/// Выбираем следующий источник.
				{
					Poco::ScopedLock<Poco::FastMutex> lock(parent.mutex);

					/// Если свободных источников нет, то этот поток больше не нужен. (Но другие потоки могут работать со своими источниками.)
					if (parent.input_queue.empty())
						break;

					input = parent.input_queue.front();

					/// Убираем источник из очереди доступных источников.
					parent.input_queue.pop();
				}

				/// Основная работа.
				Block block = input.in->read();

				{
					Poco::ScopedLock<Poco::FastMutex> lock(parent.mutex);

					/// Если этот источник ещё не иссяк, то положим полученный блок в очередь готовых.
					if (block)
					{
						parent.input_queue.push(input);
						parent.output_queue.push(block);
					}
					else
					{
						++parent.exhausted_inputs;

						/// Если все источники иссякли.
						if (parent.exhausted_inputs == parent.children.size())
						{
							/// Отдаём в основной поток пустой блок, что означает, что данных больше нет.
							parent.output_queue.push(OutputData());
							parent.finish = true;

							break;
						}
					}
				}
			}
		}

	private:
		UnionBlockInputStream & parent;
	};


	unsigned max_threads;

	/// Потоки.
	typedef std::list<ThreadData> ThreadsData;
	ThreadsData threads_data;

	/// Очередь доступных источников, которые не заняты каким-либо потоком в данный момент.
	typedef std::queue<InputData> InputQueue;
	InputQueue input_queue;

	/// Блок или эксепшен.
	struct OutputData
	{
		Block block;
		ExceptionPtr exception;

		OutputData() {}
		OutputData(Block & block_) : block(block_) {}
		OutputData(ExceptionPtr & exception_) : exception(exception_) {}
	};

	/// Очередь готовых блоков. Также туда можно положить эксепшен вместо блока.
	typedef ConcurrentBoundedQueue<OutputData> OutputQueue;
	OutputQueue output_queue;

	/// Для операций с очередями.
	Poco::FastMutex mutex;

	/// Сколько источников иссякло.
	size_t exhausted_inputs;

	/// Завершить работу потоков (раньше, чем иссякнут источники).
	volatile bool finish;
	bool all_read;

	Logger * log;
};

}
