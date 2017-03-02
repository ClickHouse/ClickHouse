#pragma once

#include <DB/IO/ReadBufferFromMemory.h>


namespace DB
{

/** Allows to read from std::string-like object.
  */
class ReadBufferFromString : public ReadBufferFromMemory
{
public:
	/// std::string or mysqlxx::Value
	template <typename S>
	ReadBufferFromString(const S & s) : ReadBufferFromMemory(s.data(), s.size()) {}
};

}
