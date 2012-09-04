#pragma once

#include <Poco/Semaphore.h>
#include <Poco/ThreadPool.h>

#include <statdaemons/threadpool.hpp>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;



/** Объединяет несколько источников в один.
  * Блоки из разных источников перемежаются друг с другом произвольным образом.
  * Можно указать количество потоков (max_threads),
  *  в которых будет выполняться получение данных из разных источников.
  */
class UnionBlockInputStream : public IProfilingBlockInputStream
{
	class Thread;
public:
	UnionBlockInputStream(BlockInputStreams inputs_, unsigned max_threads_ = 1)
		: max_threads(std::min(inputs_.size(), static_cast<size_t>(max_threads_))),
		pool(max_threads, max_threads),
		ready_any(0, inputs_.size())
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());

		for (size_t i = 0; i < inputs_.size(); ++i)
		{
			inputs_data.push_back(InputData());
			inputs_data.back().in = inputs_[i];
			inputs_data.back().i = i;
		}
	}

	Block readImpl()
	{
		Block res;

		while (1)
		{
			{
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);

				if (inputs_data.empty())
					return res;

				ssize_t max_threads_to_start = pool.available();
				
				if (max_threads_to_start > 0)
				{
					/// Запустим вычисления для как можно большего количества источников, которые ещё ни разу не брались
	//				std::cerr << "Starting initial threads" << std::endl;

					ssize_t started_threads = 0;
					InputsData::iterator it = inputs_data.begin();
					while (it != inputs_data.end() && 0 == it->count)
					{
	//					std::cerr << "Scheduling initial " << it->i << std::endl;
						++it->count;
						++started_threads;

						it->thread = new Thread(*this, inputs_data.back());
						Thread & thread = *it->thread;

						/// Переносим этот источник в конец списка
						inputs_data.push_back(*it);
						inputs_data.erase(it++);
												
						pool.start(thread);

						if (started_threads == max_threads_to_start)
							break;
					}
				}
			}

	//		std::cerr << "Waiting for one thread to finish" << std::endl;
			ready_any.wait();

			{
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	//			std::cerr << std::endl << "pool.pending: " << pool.pending() << ", pool.active: " << pool.active() << ", pool.size: " << pool.size() << std::endl;
	
				if (inputs_data.empty())
					return res;

	//			std::cerr << "Searching for first ready block" << std::endl;

				/** Найдём и вернём готовый непустой блок, если такой есть.
				  * При чём, выберем блок из источника, из которого было получено меньше всего блоков.
				  */
				InputsData::iterator it = inputs_data.begin();
				while (it != inputs_data.end())
				{
					if (it->exception)
						it->exception->rethrow();

					if (it->ready)
					{
						if (!it->block)
							inputs_data.erase(it++);
						else
							break;
					}
					else
						++it;
				}

				if (it == inputs_data.end())
				{
	//				std::cerr << "Continue" << std::endl;
					continue;
				}

	//			std::cerr << "Found block " << it->i << std::endl;

				res = it->block;

				/// Запустим получение следующего блока
				it->reset();
	//			std::cerr << "Scheduling again " << it->i << std::endl;
				++it->count;
				it->thread = new Thread(*this, *it);
				pool.start(*it->thread);

				return res;
			}
		}
	}

	String getName() const { return "UnionBlockInputStream"; }

	BlockInputStreamPtr clone() { return new UnionBlockInputStream(children, max_threads); }

	~UnionBlockInputStream()
	{
		pool.joinAll();
	}

private:
	unsigned max_threads;

	/** Будем использовать Poco::ThreadPool вместо boost::threadpool.
	  * Последний неудобен тем, что в нём не совсем так как надо узнаётся количество свободных потоков.
	  */
	Poco::ThreadPool pool;

	/// Данные отдельного источника
	struct InputData
	{
		SharedPtr<Thread> thread;
		BlockInputStreamPtr in;
		unsigned count;	/// Сколько блоков было вычислено
		bool ready;		/// Блок уже вычислен
		Block block;
		ExceptionPtr exception;
		size_t i;		/// Порядковый номер источника.

		void reset()
		{
			ready = false;
			block = Block();
			exception = NULL;
			thread = NULL;
		}

		InputData() : count(0), ready(false), i(0) {}
	};

	class Thread : public Poco::Runnable
	{
	public:
		Thread(UnionBlockInputStream & parent_, InputData & data_) : parent(parent_), data(data_) {}

		void run()
		{
			try
			{
				Block block = data.in->read();

				{
					Poco::ScopedLock<Poco::FastMutex> lock(parent.mutex);
					data.ready = true;
					data.block = block;
				}
			}
			catch (const Exception & e)
			{
				data.exception = e.clone();
			}
			catch (const Poco::Exception & e)
			{
				data.exception = e.clone();
			}
			catch (const std::exception & e)
			{
				data.exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
			}
			catch (...)
			{
				data.exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
			}

			parent.ready_any.set();
		}

	private:
		UnionBlockInputStream & parent;
		InputData & data;
	};

	/// Список упорядочен по количеству полученных из источника блоков.
	typedef std::list<InputData> InputsData;
	InputsData inputs_data;
	Poco::FastMutex mutex;
	Poco::Semaphore ready_any;
};

}
