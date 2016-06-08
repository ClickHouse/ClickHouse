#pragma once

#include <string>
#include <vector>

#define STACK_TRACE_MAX_DEPTH 32


/// Позволяет получить стек-трейс
class StackTrace
{
public:
	/// Стектрейс снимается в момент создания объекта
	StackTrace();

	/// Вывести в строку
	std::string toString() const;

private:
	using Frame = void*;
	Frame frames[STACK_TRACE_MAX_DEPTH];
	size_t frames_size;
};
