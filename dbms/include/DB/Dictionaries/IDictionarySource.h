#pragma once

#include <DB/DataStreams/IBlockInputStream.h>
#include <vector>

namespace DB
{

class IDictionarySource;
using DictionarySourcePtr = std::unique_ptr<IDictionarySource>;

/** Data-provider interface for external dictionaries,
*	abstracts out the data source (file, MySQL, ClickHouse, external program, network request et cetera)
*	from the presentation and memory layout (the dictionary itself).
*/
class IDictionarySource
{
public:
	/// returns an input stream with all the data available from this source
	virtual BlockInputStreamPtr loadAll() = 0;

	/** Indicates whether this source supports "random access" loading of data
	*	loadId and loadIds can only be used if this function returns true.
	*/
	virtual bool supportsSelectiveLoad() const = 0;

	/// returns an input stream with the data for a collection of identifiers
	virtual BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> & ids) = 0;

	/// indicates whether the source has been modified since last load* operation
	virtual bool isModified() const = 0;

	virtual DictionarySourcePtr clone() const = 0;

	/// returns an informal string describing the source
	virtual std::string toString() const = 0;

	virtual ~IDictionarySource() = default;
};

}
