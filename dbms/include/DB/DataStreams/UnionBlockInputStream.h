#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Common/ConcurrentBoundedQueue.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/ParallelInputsProcessor.h>


namespace DB
{

using Poco::SharedPtr;


/** Объединяет несколько источников в один.
  * Блоки из разных источников перемежаются друг с другом произвольным образом.
  * Можно указать количество потоков (max_threads),
  *  в которых будет выполняться получение данных из разных источников.
  *
  * Устроено так:
  * - с помощью ParallelInputsProcessor в нескольких потоках вынимает из источников блоки;
  * - полученные блоки складываются в ограниченную очередь готовых блоков;
  * - основной поток вынимает готовые блоки из очереди готовых блоков.
  */


class UnionBlockInputStream : public IProfilingBlockInputStream
{
public:
	UnionBlockInputStream(BlockInputStreams inputs, BlockInputStreamPtr additional_input_at_end, size_t max_threads) :
		output_queue(std::min(inputs.size(), max_threads)),
		handler(*this),
		processor(inputs, additional_input_at_end, max_threads, handler)
	{
		children = inputs;
		if (additional_input_at_end)
			children.push_back(additional_input_at_end);
	}

	String getName() const override { return "Union"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Union(";

		Strings children_ids(children.size());
		for (size_t i = 0; i < children.size(); ++i)
			children_ids[i] = children[i]->getID();

		/// Порядок не имеет значения.
		std::sort(children_ids.begin(), children_ids.end());

		for (size_t i = 0; i < children_ids.size(); ++i)
			res << (i == 0 ? "" : ", ") << children_ids[i];

		res << ")";
		return res.str();
	}


	~UnionBlockInputStream() override
	{
		try
		{
			if (!all_read)
				cancel();

			finalize();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

	/** Отличается от реализации по-умолчанию тем, что пытается остановить все источники,
	  *  пропуская отвалившиеся по эксепшену.
	  */
	void cancel() override
	{
		bool old_val = false;
		if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
			return;

		//std::cerr << "cancelling\n";
		processor.cancel();
	}


protected:
	void finalize()
	{
		if (!started)
			return;

		LOG_TRACE(log, "Waiting for threads to finish");

		ExceptionPtr exception;
		if (!all_read)
		{
			/** Прочитаем всё до конца, чтобы ParallelInputsProcessor не заблокировался при попытке вставить в очередь.
			  * Может быть, в очереди есть ещё эксепшен.
			  */
			OutputData res;
			while (true)
			{
				//std::cerr << "popping\n";
				output_queue.pop(res);

				if (res.exception)
				{
					if (!exception)
						exception = res.exception;
					else if (DB::Exception * e = dynamic_cast<DB::Exception *>(&*exception))
						e->addMessage("\n" + res.exception->displayText());
				}
				else if (!res.block)
					break;
			}

			all_read = true;
		}

		processor.wait();

		LOG_TRACE(log, "Waited for threads to finish");

		if (exception)
			exception->rethrow();
	}

	/** Возможны следующие варианты:
	  * 1. Функция readImpl вызывается до тех пор, пока она не вернёт пустой блок.
	  *  Затем вызывается функция readSuffix и затем деструктор.
	  * 2. Вызывается функция readImpl. В какой-то момент, возможно из другого потока вызывается функция cancel.
	  *  Затем вызывается функция readSuffix и затем деструктор.
	  * 3. В любой момент, объект может быть и так уничтожен (вызываться деструктор).
	  */

	Block readImpl() override
	{
		OutputData res;
		if (all_read)
			return res.block;

		/// Запускаем потоки, если это ещё не было сделано.
		if (!started)
		{
			started = true;
			processor.process();
		}

		/// Будем ждать, пока будет готов следующий блок или будет выкинуто исключение.
		//std::cerr << "popping\n";
		output_queue.pop(res);

		if (res.exception)
			res.exception->rethrow();

		if (!res.block)
			all_read = true;

		return res.block;
	}

	/// Вызывается либо после того, как всё прочитано, либо после cancel-а.
	void readSuffix() override
	{
		//std::cerr << "readSuffix\n";
		if (!all_read && !is_cancelled.load(std::memory_order_seq_cst))
			throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

		finalize();

		for (size_t i = 0; i < children.size(); ++i)
			children[i]->readSuffix();
	}

private:
	/// Блок или эксепшен.
	struct OutputData
	{
		Block block;
		ExceptionPtr exception;

		OutputData() {}
		OutputData(Block & block_) : block(block_) {}
		OutputData(ExceptionPtr & exception_) : exception(exception_) {}
	};

	/** Очередь готовых блоков. Также туда можно положить эксепшен вместо блока.
	  * Когда данные закончатся - в очередь вставляется пустой блок.
	  * В очередь всегда (даже после исключения или отмены запроса) рано или поздно вставляется пустой блок.
	  * Очередь всегда (даже после исключения или отмены запроса, даже в деструкторе) нужно дочитывать до пустого блока,
	  *  иначе ParallelInputsProcessor может заблокироваться при вставке в очередь.
	  */
	typedef ConcurrentBoundedQueue<OutputData> OutputQueue;
	OutputQueue output_queue;


	struct Handler
	{
		Handler(UnionBlockInputStream & parent_) : parent(parent_) {}

		void onBlock(Block & block, size_t thread_num)
		{
			//std::cerr << "pushing block\n";
			parent.output_queue.push(block);
		}

		void onFinish()
		{
			//std::cerr << "pushing end\n";
			parent.output_queue.push(OutputData());
		}

		void onException(ExceptionPtr & exception, size_t thread_num)
		{
			//std::cerr << "pushing exception\n";

			/// Порядок строк имеет значение. Если его поменять, то возможна ситуация,
			///  когда перед эксепшеном, в очередь окажется вставлен пустой блок (конец данных),
			///  и эксепшен потеряется.

			parent.output_queue.push(exception);
			parent.cancel();	/// Не кидает исключений.
		}

		UnionBlockInputStream & parent;
	};

	Handler handler;
	ParallelInputsProcessor<Handler> processor;

	bool started = false;
	bool all_read = false;

	Logger * log = &Logger::get("UnionBlockInputStream");
};

}
