#pragma once

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

struct BlockIO
{
	BlockInputStreamPtr in;
	BlockOutputStreamPtr out;
};

}
