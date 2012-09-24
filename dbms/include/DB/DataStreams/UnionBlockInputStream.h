#pragma once

#include <list>
#include <queue>

#include <Poco/Thread.h>
#include <Poco/Mutex.h>
#include <Poco/Semaphore.h>

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Очень простая thread-safe очередь ограниченной длины.
  * Если пытаться вынуть элемент из пустой очереди, то поток блокируется, пока очередь не станет непустой.
  * Если пытаться вставить элемент в переполненную очередь, то поток блокируется, пока в очереди не появится элемент.
  */
template <typename T>
class ConcurrentBoundedQueue
{
private:
	size_t max_fill;
	std::queue<T> queue;
	Poco::Mutex mutex;
	Poco::Semaphore fill_count;
	Poco::Semaphore empty_count;

public:
	ConcurrentBoundedQueue(size_t max_fill)
		: fill_count(0, max_fill), empty_count(max_fill, max_fill) {}

	void push(const T & x)
	{
		empty_count.wait();
		{
			Poco::ScopedLock<Poco::Mutex> lock(mutex);
			queue.push(x);
		}
		fill_count.set();
	}

	void pop(T & x)
	{
		fill_count.wait();
		{
			Poco::ScopedLock<Poco::Mutex> lock(mutex);
			x = queue.front();
			queue.pop();
		}
		empty_count.set();
	}
};


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
		output_queue(max_threads), exhausted_inputs(0), finish(false),
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

	Block readImpl()
	{
		Block res;
		if (finish)
			return res;
		
		/// Запускаем потоки, если это ещё не было сделано.
		if (threads_data.empty())
		{
			threads_data.resize(max_threads);
			for (ThreadsData::iterator it = threads_data.begin(); it != threads_data.end(); ++it)
			{
				it->runnable = new Thread(*this, it->exception);
				it->thread = new Poco::Thread;
				it->thread->start(*it->runnable);
			}
		}

		/// Будем ждать, пока будет готов следующий блок.
		output_queue.pop(res);

		return res;
	}

	String getName() const { return "UnionBlockInputStream"; }

	BlockInputStreamPtr clone() { return new UnionBlockInputStream(children, max_threads); }

	~UnionBlockInputStream()
	{
		LOG_TRACE(log, "Waiting for threads to finish");

		finish = true;
		for (ThreadsData::iterator it = threads_data.begin(); it != threads_data.end(); ++it)
		{
			it->thread->join();

			if (!std::uncaught_exception() && it->exception)
				it->exception->rethrow();
		}

		LOG_TRACE(log, "Waited for threads to finish");
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
		ExceptionPtr exception;
	};


	class Thread : public Poco::Runnable
	{
	public:
		Thread(UnionBlockInputStream & parent_, ExceptionPtr & exception_) : parent(parent_), exception(exception_) {}

		void run()
		{
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

				/// Отдаём в основной поток пустой блок, что означает, что данных больше нет.
				Block empty_block;
				parent.output_queue.push(empty_block);
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
							Block empty_block;
							parent.output_queue.push(empty_block);
							parent.finish = true;

							break;
						}
					}
				}
			}
		}

	private:
		UnionBlockInputStream & parent;
		ExceptionPtr & exception;
	};


	unsigned max_threads;

	/// Потоки.
	typedef std::list<ThreadData> ThreadsData;
	ThreadsData threads_data;

	/// Очередь доступных источников, которые не заняты каким-либо потоком в данный момент.
	typedef std::queue<InputData> InputQueue;
	InputQueue input_queue;

	/// Очередь готовых блоков.
	typedef ConcurrentBoundedQueue<Block> OutputQueue;
	OutputQueue output_queue;

	/// Для операций с очередями.
	Poco::FastMutex mutex;

	/// Сколько источников иссякло.
	size_t exhausted_inputs;

	/// Завершить работу потоков (раньше, чем иссякнут источники).
	volatile bool finish;

	Logger * log;
};

}
