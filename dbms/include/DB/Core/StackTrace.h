#pragma once

#include <string>
#include <vector>


namespace DB
{

/// Позволяет получить стек-трейс
class StackTrace
{
public:
	/// Стектрейс снимается в момент создания объекта
	StackTrace();

	/// Вывести в строку
	std::string toString() const;

private:
	typedef void* Frame;
	typedef std::vector<Frame> Frames;
	Frames frames;
};

}
