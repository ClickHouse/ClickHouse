#pragma once

#include <limits>

#include <DB/Common/ConcurrentBoundedQueue.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{


/** Является одновременно InputStream и OutputStream.
  * При записи, кладёт блоки в очередь.
  * При чтении, вынимает их из очереди.
  * Используется thread-safe очередь.
  * Если очередь пуста - чтение блокируется.
  * Если очередь переполнена - запись блокируется.
  *
  * Используется для того, чтобы временно сохранить куда-то результат, и позже передать его дальше.
  * Также используется для синхронизации, когда нужно из одного источника сделать несколько
  *  - для однопроходного выполнения сразу нескольких запросов.
  * Также может использоваться для распараллеливания: несколько потоков кладут блоки в очередь, а один - вынимает.
  */

class QueueBlockIOStream : public IProfilingBlockInputStream, public IBlockOutputStream
{
public:
	QueueBlockIOStream(size_t queue_size_ = std::numeric_limits<int>::max())
		: queue_size(queue_size_), queue(queue_size) {}

	String getName() const override { return "QueueBlockIOStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

	void write(const Block & block) override
	{
		queue.push(block);
	}

	void cancel() override
	{
		IProfilingBlockInputStream::cancel();
		queue.clear();
	}

protected:
	Block readImpl() override
	{
		Block res;
		queue.pop(res);
		return res;
	}

private:
	size_t queue_size;

	typedef ConcurrentBoundedQueue<Block> Queue;
	Queue queue;
};

}
