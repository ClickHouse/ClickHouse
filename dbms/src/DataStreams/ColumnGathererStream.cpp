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
										   const MergedRowSources & row_source_, size_t block_preferred_size_)
: name(column_name_), row_source(row_source_), block_preferred_size(block_preferred_size_)
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

	size_t global_size = row_source.size();
	size_t curr_block_preferred_size = std::min(global_size - pos_global_start,  block_preferred_size);
	column_res.reserve(curr_block_preferred_size);

	size_t pos_global = pos_global_start;
	while (pos_global < global_size && column_res.size() < curr_block_preferred_size)
	{
		auto source_data = row_source[pos_global].getData();
		bool source_skip = row_source[pos_global].getSkipFlag();
		auto source_num = row_source[pos_global].getSourceNum();
		Source & source = sources[source_num];

		if (source.pos >= source.size) /// Fetch new block from source_num part
		{
			fetchNewBlock(source, source_num);
		}

		/// Consecutive optimization. TODO: precompute lens
		size_t len = 1;
		size_t max_len = std::min(global_size - pos_global, source.size - source.pos); // interval should be in the same block
		for (; len < max_len && source_data == row_source[pos_global + len].getData(); ++len);

		if (!source_skip)
		{
			/// Whole block could be produced via copying pointer from current block
			if (source.pos == 0 && source.size == len)
			{
				/// If current block already contains data, return it. We will be here again on next read() iteration.
				if (column_res.size() != 0)
					break;

				block_res.unsafeGetByPosition(0).column = source.block.getByName(name).column;
				source.pos += len;
				pos_global += len;
				break;
			}
			else if (len == 1)
			{
				column_res.insertFrom(*source.column, source.pos);
			}
			else
			{
				column_res.insertRangeFrom(*source.column, source.pos, len);
			}
		}

		source.pos += len;
		pos_global += len;
	}
	pos_global_start = pos_global;

	return block_res;
}


void ColumnGathererStream::fetchNewBlock(Source & source, size_t source_num)
{
	try
	{
		source.block = children[source_num]->read();
		source.update(name);
	}
	catch (Exception & e)
	{
		e.addMessage("Cannot fetch required block. Stream " + children[source_num]->getID() + ", part " + toString(source_num));
		throw;
	}

	if (0 == source.size)
	{
		throw Exception("Fetched block is empty. Stream " + children[source_num]->getID() + ", part " + toString(source_num),
						ErrorCodes::RECEIVED_EMPTY_DATA);
	}
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
