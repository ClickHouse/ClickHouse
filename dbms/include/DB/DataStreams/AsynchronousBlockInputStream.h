#pragma once

#include <Poco/Mutex.h>
#include <Poco/Thread.h>
#include <Poco/Runnable.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** thread-safe очередь из одного элемента,
  * рассчитанная на одного producer-а и одного consumer-а.
  */
template <typename T>
class OneElementQueue
{
private:
	T data;
	Poco::FastMutex mutex_fill;		/// Захвачен, когда данные есть.
	Poco::FastMutex mutex_empty;	/// Захвачен, когда данных нет.

public:
	OneElementQueue()
	{
		mutex_empty.lock();
	}

	/// Вызывается единственным producer-ом.
	void push(const T & x)
	{
		mutex_fill.lock();
 		data = x;
		mutex_empty.unlock();
	}

	/// Вызывается единственным consumer-ом.
	void pop(T & x)
	{
		mutex_empty.lock();
		x = data;
		mutex_fill.unlock();
	}

	/// Позволяет ждать элемента не дольше заданного таймаута. Вызывается единственным consumer-ом.
	bool poll(UInt64 milliseconds)
	{
		if (mutex_empty.tryLock(milliseconds))
		{
			mutex_empty.unlock();
			return true;
		}

		return false;
	}
};


/** Выполняет другой BlockInputStream в отдельном потоке.
  * Это служит для двух целей:
  * 1. Позволяет сделать так, чтобы разные стадии конвеьера выполнения запроса работали параллельно.
  * 2. Позволяет не ждать до того, как данные будут готовы, а периодически проверять их готовность без блокировки.
  *    Это нужно, например, чтобы можно было во время ожидания проверить, не пришёл ли по сети пакет с просьбой прервать выполнение запроса.
  *    Также это позволяет выполнить несколько запросов одновременно.
  */
class AsynchronousBlockInputStream : public IProfilingBlockInputStream
{
public:
	AsynchronousBlockInputStream(BlockInputStreamPtr in_) : in(in_), started(false), runnable(*this)
	{
		children.push_back(in);
	}


	/** Ждать готовность данных не более заданного таймаута. Запустить получение данных, если нужно.
	  * Если функция вернула true - данные готовы и можно делать read().
	  */
	bool poll(UInt64 milliseconds)
	{
		startIfNeed();
		return output_queue.poll(milliseconds);
	}
	

	String getName() const { return "AsynchronousBlockInputStream"; }

	BlockInputStreamPtr clone() { return new AsynchronousBlockInputStream(in); }
	

    ~AsynchronousBlockInputStream()
	{
		if (started)
			thread->join();
	}

protected:
	Block readImpl()
	{
		OutputData res;

		startIfNeed();

		/// Будем ждать, пока будет готов следующий блок или будет выкинуто исключение.
		output_queue.pop(res);

		if (res.exception)
			res.exception->rethrow();

		return res.block;
	}
	
	
	void startIfNeed()
	{
		if (!started)
		{
			thread = new Poco::Thread;
			thread->start(runnable);
		}
	}


	/// Вычисления, которые могут выполняться в отдельном потоке
	class Thread : public Poco::Runnable
	{
	public:
		Thread(AsynchronousBlockInputStream & parent_) : parent(parent_) {}

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
				parent.cancel();

				/// Отдаём эксепшен в основной поток.
				parent.output_queue.push(exception);
			}
		}

		void loop()
		{
			while (Block res = parent.in->read())
				parent.output_queue.push(res);
		}

	private:
		AsynchronousBlockInputStream & parent;
	};


	BlockInputStreamPtr in;
	bool started;

	struct OutputData
	{
		Block block;
		ExceptionPtr exception;

		OutputData() {}
		OutputData(Block & block_) : block(block_) {}
		OutputData(ExceptionPtr & exception_) : exception(exception_) {}
	};

	OneElementQueue<OutputData> output_queue;

	Thread runnable;
	SharedPtr<Poco::Thread> thread;
};

}

