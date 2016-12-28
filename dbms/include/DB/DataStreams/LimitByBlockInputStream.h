#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Common/HashTable/HashMap.h>
#include <DB/Common/SipHash.h>
#include <DB/Common/UInt128.h>

namespace DB
{

class LimitByBlockInputStream : public IProfilingBlockInputStream
{
public:
	LimitByBlockInputStream(BlockInputStreamPtr input_, size_t value_, Names columns_);

	String getName() const override { return "LimitBy"; }

	String getID() const override;

protected:
	Block readImpl() override;

private:
	using MapHashed = HashMap<UInt128, UInt64, UInt128TrivialHash>;

	Names columns_names;
	size_t value;

	MapHashed set;
};

}
