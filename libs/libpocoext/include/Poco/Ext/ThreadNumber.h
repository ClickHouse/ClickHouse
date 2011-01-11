#ifndef THREAD_NUMBER_H
#define THREAD_NUMBER_H

/** Последовательный номер потока, начиная с 1, среди тех потоков, для которых был получен этот номер.
  * Используется при логгировании.
  */
namespace Poco
{

namespace ThreadNumber
{
	unsigned get();
}

}

#endif
