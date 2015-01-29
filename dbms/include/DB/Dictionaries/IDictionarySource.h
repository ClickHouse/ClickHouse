#pragma once

#include <DB/DataStreams/IBlockInputStream.h>
#include <vector>

namespace DB
{

class IDictionarySource
{
public:
	virtual BlockInputStreamPtr loadAll() = 0;
	virtual BlockInputStreamPtr loadId(const std::uint64_t id) = 0;
	virtual BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> ids) = 0;
	virtual bool isModified() const = 0;

	virtual ~IDictionarySource() = default;
};

using DictionarySourcePtr = std::unique_ptr<IDictionarySource>;

}
