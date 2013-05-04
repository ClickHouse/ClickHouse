#pragma once

#include <DB/DataStreams/QueueBlockIOStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Позволяет из одного источника сделать несколько.
  * Используется для однопроходного выполнения сразу нескольких запросов.
  * 
  * Несколько полученных источников должны читаться из разных потоков!
  * Расходует O(1) оперативки (не буферизует все данные).
  * Для этого, чтения из разных полученных источников синхронизируются:
  *  чтение следующего блока блокируется, пока все источники не прочитают текущий блок.
  */
class ForkBlockInputStreams : private boost::noncopyable
{
public:
	ForkBlockInputStreams(BlockInputStreamPtr source_) : source(source_) {}

	/// Создать источник. Вызывайте функцию столько раз, сколько размноженных источников вам нужно.
	BlockInputStreamPtr createInput()
	{
		destinations.push_back(new QueueBlockIOStream(1));
		return destinations.back();
	}

	/// Перед тем, как из полученных источников можно будет читать, необходимо "запустить" эту конструкцию.
	void run()
	{
		while (1)
		{
			if (destinations.empty())
				return;

			Block block = source->read();

			for (Destinations::iterator it = destinations.begin(); it != destinations.end();)
			{
				if ((*it)->isCancelled())
				{
					destinations.erase(it++);
				}
				else
				{
					(*it)->write(block);
					++it;
				}
			}

			if (!block)
				return;
		}
	}

private:
	/// Откуда читать.
	BlockInputStreamPtr source;

	/** Размноженные источники.
	  * Сделаны на основе очереди небольшой длины.
	  * Блок из source кладётся в каждую очередь.
	  */
	typedef SharedPtr<QueueBlockIOStream> Destination;
	typedef std::list<Destination> Destinations;
	Destinations destinations;
};

typedef SharedPtr<ForkBlockInputStreams> ForkPtr;
typedef std::vector<ForkPtr> Forks;

}
