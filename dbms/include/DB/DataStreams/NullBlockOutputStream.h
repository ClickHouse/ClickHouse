#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Ничего не делает. Используется для отладки и бенчмарков.
  */
class NullBlockOutputStream : public IBlockOutputStream
{
public:
	void write(const Block & block) override {}
};

}
