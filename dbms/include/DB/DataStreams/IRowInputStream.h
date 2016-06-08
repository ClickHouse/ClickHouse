#pragma once

#include <boost/noncopyable.hpp>
#include <memory>


namespace DB
{

class Block;

/** Интерфейс потока для чтения данных по строкам.
  */
class IRowInputStream : private boost::noncopyable
{
public:
	/** Прочитать следующую строку и положить её в блок.
	  * Если строк больше нет - вернуть false.
	  */
	virtual bool read(Block & block) = 0;

	/// Прочитать разделитель
	virtual void readRowBetweenDelimiter() {};	/// разделитель между строками
	virtual void readPrefix() {};				/// разделитель перед началом результата
	virtual void readSuffix() {};				/// разделитель после конца результата

	virtual ~IRowInputStream() {}
};

using RowInputStreamPtr = std::shared_ptr<IRowInputStream>;

}
