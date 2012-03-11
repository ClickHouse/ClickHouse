#pragma once

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** При чтении сразу возвращает пустой блок, что обозначает конец потока блоков.
  */
class EmptyBlockInputStream : public IBlockInputStream
{
public:
	Block read() { return Block(); }
	String getName() const { return "EmptyBlockInputStream"; }
	BlockInputStreamPtr clone() { return new EmptyBlockInputStream(); }
};

}
