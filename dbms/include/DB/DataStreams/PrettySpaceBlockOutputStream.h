#pragma once

#include <DB/DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Выводит результат, выравнивая пробелами.
  */
class PrettySpaceBlockOutputStream : public PrettyBlockOutputStream
{
public:
	PrettySpaceBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_, size_t max_rows_, const Context & context_)
		: PrettyBlockOutputStream(ostr_, no_escapes_, max_rows_, context_) {}

	void write(const Block & block) override;
	void writeSuffix() override;
};

}
