#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Выводит результат в виде красивых таблиц.
  */
class PrettyBlockOutputStream : public IBlockOutputStream
{
public:
	PrettyBlockOutputStream(WriteBuffer & ostr_) : ostr(ostr_), total_rows(0) {}
	void write(const Block & block);
	BlockOutputStreamPtr clone() { return new PrettyBlockOutputStream(ostr); }

private:
	WriteBuffer & ostr;
	size_t total_rows;
};

}
