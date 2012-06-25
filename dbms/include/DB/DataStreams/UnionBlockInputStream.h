#pragma once

#include <Poco/Semaphore.h>

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
public:
	UnionBlockInputStream(BlockInputStreams inputs_, unsigned max_threads_ = 1)
		: max_threads(std::min(inputs_.size(), static_cast<size_t>(max_threads_))),
		pool(max_threads),
		ready_any(0, inputs_.size())
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());

		for (size_t i = 0; i < inputs_.size(); ++i)
		{
			threads_data.push_back(ThreadData());
			threads_data.back().in = inputs_[i];
			threads_data.back().i = i;
		}
	}

	Block readImpl()
	{
		Block res;

		while (1)
		{
			{
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);

				if (threads_data.empty())
					return res;

				ssize_t max_threads_to_start = static_cast<ssize_t>(pool.size()) - (pool.pending() + pool.active());
				
				if (max_threads_to_start > 0)
				{
					/// Запустим вычисления для как можно большего количества источников, которые ещё ни разу не брались
	//				std::cerr << "Starting initial threads" << std::endl;

					ssize_t started_threads = 0;
					ThreadsData::iterator it = threads_data.begin();
					while (it != threads_data.end() && 0 == it->count)
					{
	//					std::cerr << "Scheduling initial " << it->i << std::endl;
						++it->count;
						++started_threads;

						/// Переносим этот источник в конец списка
						threads_data.push_back(*it);
						threads_data.erase(it++);
						
						pool.schedule(boost::bind(&UnionBlockInputStream::calculate, this, boost::ref(threads_data.back())));

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
	
				if (threads_data.empty())
					return res;

	//			std::cerr << "Searching for first ready block" << std::endl;

				/** Найдём и вернём готовый непустой блок, если такой есть.
				  * При чём, выберем блок из источника, из которого было получено меньше всего блоков.
				  */
				ThreadsData::iterator it = threads_data.begin();
				while (it != threads_data.end())
				{
					if (it->exception)
						it->exception->rethrow();

					if (it->ready)
					{
						if (!it->block)
							threads_data.erase(it++);
						else
							break;
					}
					else
						++it;
				}

				if (it == threads_data.end())
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
				pool.schedule(boost::bind(&UnionBlockInputStream::calculate, this, boost::ref(*it)));

				return res;
			}
		}
	}

	String getName() const { return "UnionBlockInputStream"; }

	BlockInputStreamPtr clone() { return new UnionBlockInputStream(children, max_threads); }

	~UnionBlockInputStream()
	{
		pool.clear();
		pool.wait();
	}

private:
	unsigned max_threads;

	boost::threadpool::pool pool;

	/// Данные отдельного источника
	struct ThreadData
	{
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
		}

		ThreadData() : count(0), ready(false), i(0) {}
	};

	/// Список упорядочен по количеству полученных из источника блоков.
	typedef std::list<ThreadData> ThreadsData;
	ThreadsData threads_data;
	Poco::FastMutex mutex;
	Poco::Semaphore ready_any;

	
	/// Вычисления, которые выполняться в отдельном потоке
	void calculate(ThreadData & data)
	{
		try
		{
			Block block = data.in->read();

			{
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);
				data.ready = true;
				data.block = block;
			}

			ready_any.set();
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
	}
};

}
