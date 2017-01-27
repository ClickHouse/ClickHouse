#pragma once

#include <boost/noncopyable.hpp>
#include <memory>


namespace DB
{

class Block;

/** Interface of stream, that allows to read data by rows.
  */
class IRowInputStream : private boost::noncopyable
{
public:
	/** Read next row and append it to block.
	  * If no more rows - return false.
	  */
	virtual bool read(Block & block) = 0;

	virtual void readPrefix() {};				/// delimiter before begin of result
	virtual void readSuffix() {};				/// delimiter after end of result

	/// Skip data until next row.
	/// This is intended for text streams, that allow skipping of errors.
	/// By default - throws not implemented exception.
	virtual bool allowSyncAfterError() const { return false; }
	virtual void syncAfterError();

	virtual ~IRowInputStream() {}
};

using RowInputStreamPtr = std::shared_ptr<IRowInputStream>;

}
