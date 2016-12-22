#include <DB/DataStreams/ColumnGathererStream.h>
#include <iomanip>

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
										   const MergedRowSources & row_source_, size_t block_size_)
: name(column_name_), row_source(row_source_), block_size(block_size_)
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

	if (pos_global_start >= row_source.size())
		return Block();

	Block block_res{column.cloneEmpty()};
	IColumn & column_res = *block_res.unsafeGetByPosition(0).column;

	size_t pos_global_finish = std::min(pos_global_start + block_size, row_source.size());
	size_t curr_block_size = pos_global_finish - pos_global_start;
	column_res.reserve(curr_block_size);

	for (size_t pos_global = pos_global_start; pos_global < pos_global_finish;)
	{
		auto source_id = row_source[pos_global].getSourceNum();
		bool skip = row_source[pos_global].getSkipFlag();
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

		/// Consecutive optimization. TODO: precompute lens
		size_t len = 1;
		size_t max_len = std::min(pos_global_finish - pos_global, source.size - source.pos); // interval should be in the same block
		for (; len < max_len && row_source[pos_global].getData() == row_source[pos_global + len].getData(); ++len);

		if (!skip)
		{
			if (column_res.size() == 0 && source.pos == 0 && curr_block_size == len && source.size == len)
			{
				// Whole block could be produced via copying pointer from current block
				block_res.unsafeGetByPosition(0).column = source.block.getByName(name).column;
			}
			else
			{
				column_res.insertRangeFrom(*source.column, source.pos, len);
			}
		}

		source.pos += len;
		pos_global += len;
	}

	pos_global_start = pos_global_finish;

	return block_res;
}


void ColumnGathererStream::readSuffixImpl()
{
	const BlockStreamProfileInfo & profile_info = getProfileInfo();
	double seconds = profile_info.total_stopwatch.elapsedSeconds();
	LOG_DEBUG(log, std::fixed << std::setprecision(2)
		<< "Gathered column " << column.name << " " << column.type->getName()
		<< " (" << static_cast<double>(profile_info.bytes) / profile_info.rows << " bytes/elem.)"
		<< " in " << seconds << " sec., "
		<< profile_info.rows / seconds << " rows/sec., "
		<< profile_info.bytes / 1048576.0 / seconds << " MiB/sec.");
}

}
