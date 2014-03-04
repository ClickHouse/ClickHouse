#include <DB/DataStreams/CreatingSetsBlockInputStream.h>
#include <iomanip>

namespace DB
{

Block CreatingSetsBlockInputStream::readImpl()
{
	Block res;

	if (!created)
	{
		for (SetPtr set : sets)
		{
			createSet(set);
			if (isCancelled())
				return res;
		}
		created = true;
	}

	if (isCancelled())
		return res;

	return children.back()->read();
}

void CreatingSetsBlockInputStream::createSet(SetPtr set)
{
	LOG_TRACE(log, "Creating set");
	Stopwatch watch;

	while (Block block = set->getSource()->read())
	{
		if (isCancelled())
		{
			LOG_DEBUG(log, "Query was cancelled during set creation");
			return;
		}
		if (!set->insertFromBlock(block))
		{
			if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*set->getSource()))
				profiling_in->cancel();
			break;
		}
	}

	logProfileInfo(watch, *set->getSource(), set->size());
	set->setSource(nullptr);
}


void CreatingSetsBlockInputStream::logProfileInfo(Stopwatch & watch, IBlockInputStream & in, size_t entries)
{
	/// Выведем информацию о том, сколько считано строк и байт.
	size_t rows = 0;
	size_t bytes = 0;

	in.getLeafRowsBytes(rows, bytes);

	size_t head_rows = 0;
	if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&in))
		head_rows = profiling_in->getInfo().rows;

	if (rows != 0)
	{
		LOG_DEBUG(log, std::fixed << std::setprecision(3)
			<< "Created set with " << entries << " entries from " << head_rows << " rows."
			<< " Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
			<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");
	}
}

}
