#pragma once

#include <boost/thread.hpp>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Выполняет другой BlockInputStream в отдельном потоке, используя двойную буферизацию.
  */
class AsynchronousBlockInputStream : public IBlockInputStream
{
typedef SharedPtr<boost::thread> ThreadPtr;
	
public:
	AsynchronousBlockInputStream(BlockInputStreamPtr in_) : in(in_)
	{
		children.push_back(in);
	}

	Block read()
	{
		/// Если вычислений ещё не было - вычислим первый блок синхронно
		if (!thread)
			calculate();
		else	/// Если вычисления уже идут - подождём результата
			thread->join();

		if (exception)
			throw *exception;

		Block res = block;
		if (!res)
			return res;

		/// Запустим вычисления следующего блока
		block = Block();
		thread = new boost::thread(&AsynchronousBlockInputStream::calculate, this);

		return res;

		return in->read();
	}

	String getName() const { return "AsynchronousBlockInputStream"; }

	BlockInputStreamPtr clone() { return new AsynchronousBlockInputStream(in); }

protected:
	BlockInputStreamPtr in;
	ThreadPtr thread;

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

