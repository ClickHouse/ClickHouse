#pragma once

#include <iostream>

#include <DB/IO/WriteBuffer.h>

#define WRITE_BUFFER_FROM_STRING_INITIAL_SIZE_IF_EMPTY 32

namespace DB
{

/** Пишет данные в строку.
  * Замечение: перед использованием полученной строки, уничтожте этот объект.
  */
class WriteBufferFromString : public WriteBuffer
{
private:
	std::string & s;

	void nextImpl()
	{
		size_t old_size = s.size();
		s.resize(old_size * 2);
		internal_buffer = Buffer(reinterpret_cast<Position>(&s[old_size]), reinterpret_cast<Position>(&*s.end()));
		working_buffer = internal_buffer;
	}

	void finish()
	{
		s.resize(count());
	}

public:
	WriteBufferFromString(std::string & s_)
		: WriteBuffer(reinterpret_cast<Position>(&s_[0]), s_.size()), s(s_)
	{
		if (s.empty())
		{
			s.resize(WRITE_BUFFER_FROM_STRING_INITIAL_SIZE_IF_EMPTY);
			set(reinterpret_cast<Position>(&s[0]), s.size());
		}
	}

    virtual ~WriteBufferFromString()
	{
		finish();
	}
};

}
