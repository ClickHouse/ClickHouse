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
	AsynchronousBlockInputStream(BlockInputStreamPtr in_) : pool(1), started(false)
	{
		children.push_back(in_);
	}

	String getName() const { return "AsynchronousBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Asynchronous(" << children.back()->getID() << ")";
		return res.str();
	}

	void readPrefix()
	{
		children.back()->readPrefix();
		next();
		started = true;
	}

	void readSuffix()
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


    ~AsynchronousBlockInputStream()
	{
		if (started)
			pool.wait();
	}

protected:
	boost::threadpool::pool pool;
	Poco::Event ready;
	bool started;

	Block block;
	ExceptionPtr exception;

	
	Block readImpl()
	{
		/// Если вычислений ещё не было - вычислим первый блок синхронно
		if (!started)
		{
			calculate();
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
		pool.schedule(boost::bind(&AsynchronousBlockInputStream::calculate, this));
	}
	

	/// Вычисления, которые могут выполняться в отдельном потоке
	void calculate()
	{
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

