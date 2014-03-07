#pragma once

#include <string>
#include <vector>

#define DBMS_STACK_TRACE_MAX_DEPTH 32


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
	Frame frames[DBMS_STACK_TRACE_MAX_DEPTH];
	size_t frames_size;
};

}
