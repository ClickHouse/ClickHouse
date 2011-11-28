#pragma once

#include <DB/DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Выводит результат, выравнивая пробелами.
  */
class PrettySpaceBlockOutputStream : public PrettyBlockOutputStream
{
public:
	PrettySpaceBlockOutputStream(WriteBuffer & ostr_, size_t max_rows_ = PRETTY_FORMAT_DEFAULT_MAX_ROWS)
		: PrettyBlockOutputStream(ostr_, max_rows_) {}

	void write(const Block & block);
	void writeSuffix();
	BlockOutputStreamPtr clone() { return new PrettySpaceBlockOutputStream(ostr); }
};

}
