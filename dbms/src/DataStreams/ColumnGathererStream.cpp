#include <DB/DataStreams/ColumnGathererStream.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int INCOMPATIBLE_COLUMNS;
	extern const int INCORRECT_NUMBER_OF_COLUMNS;
	extern const int EMPTY_DATA_PASSED;
	extern const int RECEIVED_EMPTY_DATA;
}

ColumnGathererStream::ColumnGathererStream(const BlockInputStreams & source_streams, const String & column_name_,
										   const MergedRowSources & pos_to_source_idx_, size_t block_size_)
: name(column_name_), pos_to_source_idx(pos_to_source_idx_), block_size(block_size_)
{
	if (source_streams.empty())
		throw Exception("There are no streams to gather", ErrorCodes::EMPTY_DATA_PASSED);

	children.assign(source_streams.begin(), source_streams.end());

	sources.reserve(children.size());
	for (size_t i = 0; i < children.size(); i++)
	{
		sources.emplace_back(children[i]->read(), name);

		Block & block = sources.back().block;

		/// Sometimes MergeTreeReader injects additional column with partitioning key
		if (block.columns() > 2 || !block.has(name))
			throw Exception("Block should have 1 or 2 columns and contain column with requested name", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

		if (i == 0)
		{
			column.name = name;
			column.type = block.getByName(name).type->clone();
			column.column = column.type->createColumn();
		}

		if (block.getByName(name).column->getName() != column.column->getName())
			throw Exception("Column types don't match", ErrorCodes::INCOMPATIBLE_COLUMNS);
	}
}


String ColumnGathererStream::getID() const
{
	std::stringstream res;

	res << getName() << "(";
	for (size_t i = 0; i < children.size(); i++)
		res << (i == 0 ? "" : ", " ) << children[i]->getID();
	res << ")";

	return res.str();
}


Block ColumnGathererStream::readImpl()
{
	if (children.size() == 1)
		return children[0]->read();

	if (pos_global >= pos_to_source_idx.size())
		return Block();

	Block block_res{column.cloneEmpty()};
	IColumn & column_res = *block_res.unsafeGetByPosition(0).column;

	size_t pos_finish = std::min(pos_global + block_size, pos_to_source_idx.size());
	column_res.reserve(pos_finish - pos_global);

	for (size_t pos = pos_global; pos < pos_finish; ++pos)
	{
		auto source_id = pos_to_source_idx[pos].getSourceNum();
		bool skip = pos_to_source_idx[pos].getSkipFlag();
		Source & source = sources[source_id];

		if (source.pos >= source.size) /// Fetch new block
		{
			try
			{
				source.block = children[source_id]->read();
				source.update(name);
			}
			catch (Exception & e)
			{
				e.addMessage("Cannot fetch required block. Stream " + children[source_id]->getID() + ", part " + toString(source_id));
				throw;
			}

			if (0 == source.size)
			{
				throw Exception("Fetched block is empty. Stream " + children[source_id]->getID() + ", part " + toString(source_id),
								ErrorCodes::RECEIVED_EMPTY_DATA);
			}
		}

		if (!skip)
			column_res.insertFrom(*source.column, source.pos); //TODO: vectorize
		++source.pos;
	}

	pos_global = pos_finish;

	return block_res;
}

}
