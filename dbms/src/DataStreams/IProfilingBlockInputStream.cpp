#include <iomanip>

/*#include <Poco/Mutex.h>
#include <Poco/Ext/ThreadNumber.h>*/

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


void BlockStreamProfileInfo::update(Block & block)
{
	++blocks;
	rows += block.rows();
	bytes += block.bytes();

	if (column_names.empty())
		column_names = block.dumpNames();
}


void BlockStreamProfileInfo::print(std::ostream & ostr) const
{
	Poco::Timestamp::TimeDiff nested_elapsed = 0;
	UInt64 nested_rows 		= 0;
	UInt64 nested_blocks 	= 0;
	UInt64 nested_bytes 	= 0;
	
	if (!nested_infos.empty())
	{
		for (BlockStreamProfileInfos::const_iterator it = nested_infos.begin(); it != nested_infos.end(); ++it)
		{
			if ((*it)->work_stopwatch.elapsed() > nested_elapsed)
				nested_elapsed = (*it)->work_stopwatch.elapsed();
			nested_rows 	+= (*it)->rows;
			nested_blocks	+= (*it)->blocks;
			nested_bytes 	+= (*it)->bytes;
		}
	}
	
	ostr 	<< std::fixed << std::setprecision(2)
			<< "Columns: " << column_names << std::endl
			<< "Elapsed:        " << work_stopwatch.elapsed() / 1000000.0 << " sec. "
			<< "(" << work_stopwatch.elapsed() * 100.0 / total_stopwatch.elapsed() << "%), " << std::endl;

	if (!nested_infos.empty())
	{
		double self_percents = (work_stopwatch.elapsed() - nested_elapsed) * 100.0 / total_stopwatch.elapsed();
		
		ostr<< "Elapsed (self): " << (work_stopwatch.elapsed() - nested_elapsed) / 1000000.0 << " sec. "
			<< "(" << (self_percents >= 50 ? "\033[1;31m" : (self_percents >= 10 ? "\033[1;33m" : ""))	/// Раскраска больших значений
				<< self_percents << "%"
				<< (self_percents >= 10 ? "\033[0m" : "") << "), " << std::endl
			<< "Rows (in):      " << nested_rows << ", per second: " << nested_rows * 1000000 / work_stopwatch.elapsed() << ", " << std::endl
			<< "Blocks (in):    " << nested_blocks << ", per second: " << nested_blocks * 1000000.0 / work_stopwatch.elapsed() << ", " << std::endl
			<< "                " << nested_bytes / 1000000.0 << " MB (memory), "
				<< nested_bytes / work_stopwatch.elapsed() << " MB/s (memory), " << std::endl;

		if (self_percents > 0.1)
			ostr << "Rows per second (in, self): " << (nested_rows * 1000000 / (work_stopwatch.elapsed() - nested_elapsed))
				<< ", " << (work_stopwatch.elapsed() - nested_elapsed) * 1000 / nested_rows << " ns/row, " << std::endl;
	}
		
	ostr 	<< "Rows (out):     " << rows << ", per second: " << rows * 1000000 / work_stopwatch.elapsed() << ", " << std::endl
			<< "Blocks (out):   " << blocks << ", per second: " << blocks * 1000000.0 / work_stopwatch.elapsed() << ", " << std::endl
			<< "                " << bytes / 1000000.0 << " MB (memory), " << bytes / work_stopwatch.elapsed() << " MB/s (memory), " << std::endl
			<< "Average block size (out): " << rows / blocks << "." << std::endl;
}

	
Block IProfilingBlockInputStream::read()
{
	if (!info.started)
	{
		info.total_stopwatch.start();

		for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
			if (const IProfilingBlockInputStream * child = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
				info.nested_infos.push_back(&child->info);
		
		info.started = true;
	}

	if (is_cancelled_callback && is_cancelled_callback())
		return Block();

	LOG_TRACE((&Logger::get("IProfilingBlockInputStream")), "Reading from " << getName());
	
	info.work_stopwatch.start();
	Block res = readImpl();
	info.work_stopwatch.stop();

	LOG_TRACE((&Logger::get("IProfilingBlockInputStream")), "Read from" << getName());

/*	if (res)
	{
		static Poco::FastMutex mutex;
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		std::cerr << std::endl;
		std::cerr << "[ " << Poco::ThreadNumber::get() << " ]\t" << getName() << std::endl;
		std::cerr << "[ " << Poco::ThreadNumber::get() << " ]\t";

		for (size_t i = 0; i < res.columns(); ++i)
		{
			if (i != 0)
				std::cerr << ", ";
			std::cerr << res.getByPosition(i).name << " (" << res.getByPosition(i).column->size() << ")";
		}
		
		std::cerr << std::endl;
	}*/

	if (res)
		info.update(res);

	progress(res);
	
	return res;
}


void IProfilingBlockInputStream::progress(Block & block)
{
	if (children.empty() && progress_callback)
		progress_callback(block.rows(), block.bytes());
}
	

const BlockStreamProfileInfo & IProfilingBlockInputStream::getInfo() const
{
	return info;
}


void IProfilingBlockInputStream::setIsCancelledCallback(IsCancelledCallback callback)
{
	BlockInputStreams leaves = getLeaves();
	for (BlockInputStreams::iterator it = leaves.begin(); it != leaves.end(); ++it)
		if (IProfilingBlockInputStream * leaf = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			leaf->is_cancelled_callback = callback;
}


void IProfilingBlockInputStream::setProgressCallback(ProgressCallback callback)
{
	progress_callback = callback;
	
	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			child->setProgressCallback(callback);
}


}
