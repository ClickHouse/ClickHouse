#pragma once

#include <statdaemons/threadpool.hpp>

#include <Poco/Event.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

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
	AsynchronousBlockInputStream(BlockInputStreamPtr in_)
	{
		children.push_back(in_);
	}

	String getName() const override { return "AsynchronousBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Asynchronous(" << children.back()->getID() << ")";
		return res.str();
	}

	void readPrefix() override
	{
		children.back()->readPrefix();
		next();
		started = true;
	}

	void readSuffix() override
	{
		if (started)
		{
			pool.wait();
			if (exception)
				exception->rethrow();
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
	boost::threadpool::pool pool{1};
	Poco::Event ready;
	bool started = false;

	Block block;
	ExceptionPtr exception;


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
			exception->rethrow();

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
		current_memory_tracker = memory_tracker;

		try
		{
			block = children.back()->read();
		}
		catch (...)
		{
			exception = cloneCurrentException();
		}

		ready.set();
	}
};

}

