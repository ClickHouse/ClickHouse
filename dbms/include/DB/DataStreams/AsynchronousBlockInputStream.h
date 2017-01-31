#pragma once

#include <Poco/Event.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/Common/ThreadPool.h>


namespace CurrentMetrics
{
	extern const Metric QueryThread;
}

namespace DB
{

/** Выполняет другой BlockInputStream в отдельном потоке.
  * Это служит для двух целей:
  * 1. Позволяет сделать так, чтобы разные стадии конвейера выполнения запроса работали параллельно.
  * 2. Позволяет не ждать до того, как данные будут готовы, а периодически проверять их готовность без блокировки.
  *    Это нужно, например, чтобы можно было во время ожидания проверить, не пришёл ли по сети пакет с просьбой прервать выполнение запроса.
  *    Также это позволяет выполнить несколько запросов одновременно.
  */
class AsynchronousBlockInputStream : public IProfilingBlockInputStream
{
public:
	AsynchronousBlockInputStream(BlockInputStreamPtr in_)
	{
		children.push_back(in_);
	}

	String getName() const override { return "Asynchronous"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Asynchronous(" << children.back()->getID() << ")";
		return res.str();
	}

	void readPrefix() override
	{
		/// Не будем вызывать readPrefix у ребёнка, чтобы соответствующие действия совершались в отдельном потоке.
		if (!started)
		{
			next();
			started = true;
		}
	}

	void readSuffix() override
	{
		if (started)
		{
			pool.wait();
			if (exception)
				std::rethrow_exception(exception);
			children.back()->readSuffix();
			started = false;
		}
	}


	/** Ждать готовность данных не более заданного таймаута. Запустить получение данных, если нужно.
	  * Если функция вернула true - данные готовы и можно делать read(); нельзя вызвать функцию сразу ещё раз.
	  */
	bool poll(UInt64 milliseconds)
	{
		if (!started)
		{
			next();
			started = true;
		}

		return ready.tryWait(milliseconds);
	}


    ~AsynchronousBlockInputStream() override
	{
		if (started)
			pool.wait();
	}

protected:
	ThreadPool pool{1};
	Poco::Event ready;
	bool started = false;
	bool first = true;

	Block block;
	std::exception_ptr exception;


	Block readImpl() override
	{
		/// Если вычислений ещё не было - вычислим первый блок синхронно
		if (!started)
		{
			calculate(current_memory_tracker);
			started = true;
		}
		else	/// Если вычисления уже идут - подождём результата
			pool.wait();

		if (exception)
			std::rethrow_exception(exception);

		Block res = block;
		if (!res)
			return res;

		/// Запустим вычисления следующего блока
		block = Block();
		next();

		return res;
	}


	void next()
	{
		ready.reset();
		pool.schedule(std::bind(&AsynchronousBlockInputStream::calculate, this, current_memory_tracker));
	}


	/// Вычисления, которые могут выполняться в отдельном потоке
	void calculate(MemoryTracker * memory_tracker)
	{
		CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

		try
		{
			if (first)
			{
				first = false;
				setThreadName("AsyncBlockInput");
				current_memory_tracker = memory_tracker;
				children.back()->readPrefix();
			}

			block = children.back()->read();
		}
		catch (...)
		{
			exception = std::current_exception();
		}

		ready.set();
	}
};

}

