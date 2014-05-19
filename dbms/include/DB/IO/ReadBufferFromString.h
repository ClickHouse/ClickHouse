#pragma once

#include <DB/IO/ReadBuffer.h>


namespace DB
{

/** Позволяет читать из строки.
  */
class ReadBufferFromString : public ReadBuffer
{
public:
	/// std::string или mysqlxx::Value
	template <typename S>
	ReadBufferFromString(const S & s) : ReadBuffer(const_cast<char *>(s.data()), s.size(), 0) {}
};

}
