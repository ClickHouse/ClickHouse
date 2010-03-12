#ifndef DBMS_DATA_STREAMS_LIMITBLOCKINPUTSTREAM_H
#define DBMS_DATA_STREAMS_LIMITBLOCKINPUTSTREAM_H

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует реляционную операцию LIMIT.
  */
class LimitBlockInputStream : public IBlockInputStream
{
public:
	LimitBlockInputStream(SharedPtr<IBlockInputStream> input_, size_t limit_, size_t offset_ = 0);
	Block read();

private:
	SharedPtr<IBlockInputStream> input;
	size_t limit;
	size_t offset;
	size_t pos;
};

}

#endif
