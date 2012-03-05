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
		threads_data(inputs_.size()),
		ready_any(0, inputs_.size())
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
		for (size_t i = 0; i < inputs_.size(); ++i)
			threads_data[i].in = inputs_[i];
	}

	Block readImpl()
	{
		Block res;

	//	time_t current_time = time(0);
	//	std::cerr << std::endl << ctime(&current_time) << std::endl;

		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		//	std::cerr << "Starting initial threads" << std::endl;

			/// Запустим вычисления для как можно большего количества источников, которые ещё ни разу не брались
			for (size_t i = 0; i < threads_data.size(); ++i)
			{
				if (0 == threads_data[i].count)
				{
		//			std::cerr << "Scheduling " << i << std::endl;
					++threads_data[i].count;
					pool.schedule(boost::bind(&UnionBlockInputStream::calculate, this, boost::ref(threads_data[i])/*, i*/));
				}
			}
		}

		while (1)
		{
		//	std::cerr << "Waiting for one thread to finish" << std::endl;
			ready_any.wait();

		/*	std::cerr << std::endl << "pool.pending: " << pool.pending() << ", pool.active: " << pool.active() << ", pool.size: " << pool.size() << std::endl;
			for (size_t i = 0; i < threads_data.size(); ++i)
			{
				std::cerr << "\t" << "i: " << i << ", count: " << threads_data[i].count << ", ready: " << threads_data[i].ready << ", block: " << !!threads_data[i].block  << std::endl;
			}*/

			{
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);
			//	std::cerr << "Checking end" << std::endl;

				/// Если все блоки готовы и пустые
				size_t i = 0;
				for (; i < threads_data.size(); ++i)
					if (!threads_data[i].ready || threads_data[i].block)
						break;
				if (i == threads_data.size())
					return res;

			//	std::cerr << "Searching for first ready block" << std::endl;

				/** Найдём и вернём готовый непустой блок, если такой есть.
				  * При чём, выберем блок из источника, из которого было получено меньше всего блоков.
				  */
				unsigned min_count = 0;
				ssize_t argmin_i = -1;
				for (size_t i = 0; i < threads_data.size(); ++i)
				{
					if (threads_data[i].exception)
						threads_data[i].exception->rethrow();

					if (threads_data[i].ready && threads_data[i].block
						&& (argmin_i == -1 || threads_data[i].count < min_count))
					{
						min_count = threads_data[i].count;
						argmin_i = i;
					}
				}

				if (argmin_i == -1)
					continue;

			//	std::cerr << "Returning found block " << argmin_i << std::endl;

				res = threads_data[argmin_i].block;

				/// Запустим получение следующего блока
				threads_data[argmin_i].reset();
			//	std::cerr << "Scheduling " << argmin_i << std::endl;
				++threads_data[argmin_i].count;
				pool.schedule(boost::bind(&UnionBlockInputStream::calculate, this, boost::ref(threads_data[argmin_i])/*, argmin_i*/));

				return res;
			}
		}
	}

	String getName() const { return "UnionBlockInputStream"; }

	BlockInputStreamPtr clone() { return new UnionBlockInputStream(children, max_threads); }

    ~UnionBlockInputStream()
	{
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
		SharedPtr<Exception> exception;

		void reset()
		{
			ready = false;
			block = Block();
			exception = NULL;
		}

		ThreadData() : count(0), ready(false) {}
	};

	typedef std::vector<ThreadData> ThreadsData;
	ThreadsData threads_data;
	Poco::FastMutex mutex;
	Poco::Semaphore ready_any;

	
	/// Вычисления, которые выполняться в отдельном потоке
	void calculate(ThreadData & data/*, int i*/)
	{
		try
		{
		//	std::cerr << "\033[1;37m" << "Calculating " << i << "\033[0m" << std::endl;
		//	sleep(i);
			Block block = data.in->read();

			{
				Poco::ScopedLock<Poco::FastMutex> lock(mutex);
				data.ready = true;
				data.block = block;
			}

			ready_any.set();
		//	std::cerr << "\033[1;37m" << "Done " << i << "\033[0m" << std::endl;
		}
		catch (const Exception & e)
		{
			data.exception = new Exception(e);
		}
		catch (const Poco::Exception & e)
		{
			std::cerr << e.message() << std::endl;
			data.exception = new Exception(e.message(), ErrorCodes::POCO_EXCEPTION);
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
