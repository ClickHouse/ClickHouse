#pragma once

#include <string>

namespace DB
{

/** Этот интерфейс определяет функции, которые классы ReadBufferAIO и WriteBufferAIO реализуют.
  */
class IBufferAIO
{
public:
	virtual ~IBufferAIO() = default;
	virtual std::string getFileName() const noexcept = 0;
	virtual int getFD() const noexcept = 0;

public:
	static const size_t BLOCK_SIZE = 512;

protected:
	/// Ждать окончания текущей асинхронной задачи.
	virtual void waitForCompletion() = 0;
	/// Менять местами основной и дублирующий буферы.
	virtual void swapBuffers() noexcept = 0;
};

}
