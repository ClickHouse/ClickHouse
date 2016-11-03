#include <DB/DataStreams/ColumnGathererStream.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int INCOMPATIBLE_COLUMNS;
}

ColumnGathererStream::ColumnGathererStream(const BlockInputStreams & source_streams, MergedRowSources & pos_to_source_idx_, size_t block_size_)
: pos_to_source_idx(pos_to_source_idx_), block_size(block_size_)
{
	children.assign(source_streams.begin(), source_streams.end());

	sources.reserve(children.size());
	for (size_t i = 0; i < children.size(); i++)
	{
		sources.emplace_back(children[i]->read());

		Block & block = sources.back().block;
		if (block.columns() > 1 || !block.getByPosition(0).column ||
			block.getByPosition(0).type->getName() != sources[0].block.getByPosition(0).type->getName())
			throw Exception("Column formats don't match", ErrorCodes::INCOMPATIBLE_COLUMNS);
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

	if (children.size() == 0 || pos_global >= pos_to_source_idx.size())
		return Block();

	Block block_res = sources[0].block.cloneEmpty();
	IColumn * column_res = &*block_res.getByPosition(0).column;

	size_t pos_finish = std::min(pos_global + block_size, pos_to_source_idx.size());
	column_res->reserve(pos_finish - pos_global);

	for (size_t pos = pos_global; pos < pos_finish; ++pos)
	{
		auto source_id = pos_to_source_idx[pos].source_id;
		bool skip = pos_to_source_idx[pos].skip;
		Source & source = sources[source_id];

		if (source.pos >= source.size) /// Fetch new block
		{
			try
			{
				source.block = children[source_id]->read();
				source.update();
			}
			catch (...)
			{
				source.size = 0;
			}

			if (0 == source.size)
			{
				throw Exception("Can't fetch required block from " + children[source_id]->getID() + " (i. e.  source " + std::to_string(source_id) +")",
								ErrorCodes::LOGICAL_ERROR);
			}
		}

		if (!skip)
			column_res->insertFrom(*source.column, source.pos);
		++source.pos;
	}

	pos_global = pos_finish;

	return block_res;
}

}
