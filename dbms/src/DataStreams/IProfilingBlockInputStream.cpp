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
	UInt64 elapsed 			= work_stopwatch.elapsed();
	UInt64 nested_elapsed	= 0;
	double elapsed_seconds	= work_stopwatch.elapsedSeconds();
	double nested_elapsed_seconds = 0;
	
	UInt64 nested_rows 		= 0;
	UInt64 nested_blocks 	= 0;
	UInt64 nested_bytes 	= 0;
	
	if (!nested_infos.empty())
	{
		for (BlockStreamProfileInfos::const_iterator it = nested_infos.begin(); it != nested_infos.end(); ++it)
		{
			if ((*it)->work_stopwatch.elapsed() > nested_elapsed)
			{
				nested_elapsed = (*it)->work_stopwatch.elapsed();
				nested_elapsed_seconds = (*it)->work_stopwatch.elapsedSeconds();
			}
			
			nested_rows 	+= (*it)->rows;
			nested_blocks	+= (*it)->blocks;
			nested_bytes 	+= (*it)->bytes;
		}
	}
	
	ostr 	<< std::fixed << std::setprecision(2)
			<< "Columns: " << column_names << std::endl
			<< "Elapsed:        " << elapsed_seconds << " sec. "
			<< "(" << elapsed * 100.0 / total_stopwatch.elapsed() << "%), " << std::endl;

	if (!nested_infos.empty())
	{
		double self_percents = (elapsed - nested_elapsed) * 100.0 / total_stopwatch.elapsed();
		
		ostr<< "Elapsed (self): " << (elapsed_seconds - nested_elapsed_seconds) << " sec. "
			<< "(" << (self_percents >= 50 ? "\033[1;31m" : (self_percents >= 10 ? "\033[1;33m" : ""))	/// Раскраска больших значений
				<< self_percents << "%"
				<< (self_percents >= 10 ? "\033[0m" : "") << "), " << std::endl
			<< "Rows (in):      " << nested_rows << ", per second: " << nested_rows / elapsed_seconds << ", " << std::endl
			<< "Blocks (in):    " << nested_blocks << ", per second: " << nested_blocks / elapsed_seconds << ", " << std::endl
			<< "                " << nested_bytes / 1000000.0 << " MB (memory), "
				<< nested_bytes * 1000 / elapsed << " MB/s (memory), " << std::endl;

		if (self_percents > 0.1)
			ostr << "Rows per second (in, self): " << (nested_rows / (elapsed_seconds - nested_elapsed_seconds))
				<< ", " << (elapsed - nested_elapsed) / nested_rows << " ns/row, " << std::endl;
	}
		
	ostr 	<< "Rows (out):     " << rows << ", per second: " << rows / elapsed_seconds << ", " << std::endl
			<< "Blocks (out):   " << blocks << ", per second: " << blocks / elapsed_seconds << ", " << std::endl
			<< "                " << bytes / 1000000.0 << " MB (memory), " << bytes * 1000 / elapsed << " MB/s (memory), " << std::endl
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

	if (is_cancelled)
		return Block();

	info.work_stopwatch.start();
	Block res = readImpl();
	info.work_stopwatch.stop();

/*	if (res)
	{
		static Poco::FastMutex mutex;
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		std::cerr << std::endl;
		std::cerr << "[ " << Poco::ThreadNumber::get() << " ]\t" << getShortName() << std::endl;
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
	else
	{
		/** Если поток закончился, то ещё попросим всех детей прервать выполнение.
		  * Это имеет смысл при выполнении запроса с LIMIT-ом:
		  * - бывает ситуация, когда все необходимые данные уже прочитали,
		  *   но источники-дети ещё продолжают работать,
		  *   при чём они могут работать в отдельных потоках или даже удалённо.
		  */
		cancel();
	}

	progress(res);

	/// Проверка ограничений.
	if ((limits.max_rows_to_read && info.rows > limits.max_rows_to_read)
		|| (limits.max_bytes_to_read && info.bytes > limits.max_bytes_to_read))
	{
		if (limits.read_overflow_mode == Limits::THROW)
			throw Exception("Limit for rows to read exceeded: read " + Poco::NumberFormatter::format(info.rows)
				+ " rows, maximum: " + Poco::NumberFormatter::format(limits.max_rows_to_read),
				ErrorCodes::TOO_MUCH_ROWS);

		if (limits.read_overflow_mode == Limits::BREAK)
			return Block();

		throw Exception("Logical error: unkown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	if (limits.max_execution_time != 0
		&& info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
	{
		if (limits.timeout_overflow_mode == Limits::THROW)
			throw Exception("Timeout exceeded: elapsed " + Poco::NumberFormatter::format(info.total_stopwatch.elapsedSeconds())
				+ " seconds, maximum: " + Poco::NumberFormatter::format(limits.max_execution_time.totalMicroseconds() / 1000000.0),
			ErrorCodes::TIMEOUT_EXCEEDED);

		if (limits.timeout_overflow_mode == Limits::BREAK)
			return Block();

		throw Exception("Logical error: unkown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	if (limits.min_execution_speed
		&& info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.timeout_before_checking_execution_speed.totalMicroseconds()) * 1000
		&& info.rows / info.total_stopwatch.elapsedSeconds() < limits.min_execution_speed)
	{
		throw Exception("Query is executing too slow: " + Poco::NumberFormatter::format(info.rows / info.total_stopwatch.elapsedSeconds())
			+ " rows/sec., minimum: " + Poco::NumberFormatter::format(limits.min_execution_speed),
			ErrorCodes::TOO_SLOW);
	}
	
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


void IProfilingBlockInputStream::cancel()
{
	if (!__sync_bool_compare_and_swap(&is_cancelled, false, true))
		return;

	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			child->cancel();
}


void IProfilingBlockInputStream::setProgressCallback(ProgressCallback callback)
{
	progress_callback = callback;
	
	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			child->setProgressCallback(callback);
}


}
