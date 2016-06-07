#pragma once

#include <DB/DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Выводит результат, выравнивая пробелами.
  */
class PrettySpaceBlockOutputStream : public PrettyBlockOutputStream
{
public:
	PrettySpaceBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_ = false, size_t max_rows_ = PRETTY_FORMAT_DEFAULT_MAX_ROWS)
		: PrettyBlockOutputStream(ostr_, no_escapes_, max_rows_) {}

	void write(const Block & block) override;
	void writeSuffix() override;
};

}
