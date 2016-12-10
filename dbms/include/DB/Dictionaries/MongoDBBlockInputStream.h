#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Dictionaries/ExternalResultDescription.h>


namespace mongo
{
	class DBClientCursor;
}


namespace DB
{

/// Converts mongo::DBClientCursor to a stream of DB::Block`s
class MongoDBBlockInputStream final : public IProfilingBlockInputStream
{
public:
	MongoDBBlockInputStream(
		std::unique_ptr<mongo::DBClientCursor> cursor_, const Block & sample_block, const size_t max_block_size);

    ~MongoDBBlockInputStream() override;

	String getName() const override { return "MongoDB"; }

	String getID() const override;

private:
	Block readImpl() override;

	static void insertDefaultValue(IColumn * column, const IColumn & sample_column)
	{
		column->insertFrom(sample_column, 0);
	}

	std::unique_ptr<mongo::DBClientCursor> cursor;
	const size_t max_block_size;
	ExternalResultDescription description;
};

}
