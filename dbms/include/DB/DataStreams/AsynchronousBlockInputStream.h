#pragma once

#include <statdaemons/threadpool.hpp>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Выполняет другой BlockInputStream в отдельном потоке, используя двойную буферизацию.
  */
class AsynchronousBlockInputStream : public IProfilingBlockInputStream
{
public:
	AsynchronousBlockInputStream(BlockInputStreamPtr in_) : in(in_), pool(1), started(false)
	{
		children.push_back(in);
	}

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
		pool.schedule(boost::bind(&AsynchronousBlockInputStream::calculate, this));

		return res;
	}

	String getName() const { return "AsynchronousBlockInputStream"; }

	BlockInputStreamPtr clone() { return new AsynchronousBlockInputStream(in); }

protected:
	BlockInputStreamPtr in;
	boost::threadpool::pool pool;
	bool started;

	Block block;
	SharedPtr<Exception> exception;

	/// Вычисления, которые могут выполняться в отдельном потоке
	void calculate()
	{
		try
		{
			block = in->read();
		}
		catch (const Exception & e)
		{
			exception = new Exception(e);
		}
		catch (const Poco::Exception & e)
		{
			exception = new Exception(e.message(), ErrorCodes::POCO_EXCEPTION);
		}
		catch (const std::exception & e)
		{
			exception = new Exception(e.what(), ErrorCodes::STD_EXCEPTION);
		}
		catch (...)
		{
			exception = new Exception("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
		}
	}
};

}

