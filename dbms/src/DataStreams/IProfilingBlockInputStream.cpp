#include <iomanip>

/*#include <Poco/Mutex.h>
#include <Poco/Ext/ThreadNumber.h>*/

#include <DB/Columns/ColumnConst.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


void BlockStreamProfileInfo::read(ReadBuffer & in)
{
	readVarUInt(rows, in);
	readVarUInt(blocks, in);
	readVarUInt(bytes, in);
	readBinary(applied_limit, in);
	readVarUInt(rows_before_limit, in);
	readBinary(calculated_rows_before_limit, in);	
}


void BlockStreamProfileInfo::write(WriteBuffer & out) const
{
	writeVarUInt(rows, out);
	writeVarUInt(blocks, out);
	writeVarUInt(bytes, out);
	writeBinary(hasAppliedLimit(), out);
	writeVarUInt(getRowsBeforeLimit(), out);
	writeBinary(calculated_rows_before_limit, out);
}


size_t BlockStreamProfileInfo::getRowsBeforeLimit() const
{
	if (!calculated_rows_before_limit)
		calculateRowsBeforeLimit();
	return rows_before_limit;
}


bool BlockStreamProfileInfo::hasAppliedLimit() const
{
	if (!calculated_rows_before_limit)
		calculateRowsBeforeLimit();
	return applied_limit;
}	


void BlockStreamProfileInfo::update(Block & block)
{
	++blocks;
	rows += block.rows();
	bytes += block.bytes();

	if (column_names.empty())
		column_names = block.dumpNames();
}


void BlockStreamProfileInfo::collectInfosForStreamsWithName(const String & name, BlockStreamProfileInfos & res) const
{
	if (stream_name == name)
	{
		res.push_back(this);
		return;
	}

	for (BlockStreamProfileInfos::const_iterator it = nested_infos.begin(); it != nested_infos.end(); ++it)
		(*it)->collectInfosForStreamsWithName(name, res);
}


void BlockStreamProfileInfo::calculateRowsBeforeLimit() const
{
	calculated_rows_before_limit = true;
	
	/// есть ли Limit?
	BlockStreamProfileInfos limits;
	collectInfosForStreamsWithName("Limit", limits);
	if (limits.empty())
		return;

	applied_limit = true;

	/** Берём количество строчек, прочитанных ниже PartialSorting-а, если есть, или ниже Limit-а.
	  * Это нужно, потому что сортировка может вернуть только часть строк.
	  */
	BlockStreamProfileInfos partial_sortings;
	collectInfosForStreamsWithName("PartialSorting", partial_sortings);

	BlockStreamProfileInfos & limits_or_sortings = partial_sortings.empty() ? limits : partial_sortings;

	for (BlockStreamProfileInfos::const_iterator it = limits_or_sortings.begin(); it != limits_or_sortings.end(); ++it)
		for (BlockStreamProfileInfos::const_iterator jt = (*it)->nested_infos.begin(); jt != (*it)->nested_infos.end(); ++jt)
			rows_before_limit += (*jt)->rows;
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
		info.stream_name = getShortName();

		for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
			if (const IProfilingBlockInputStream * child = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
				info.nested_infos.push_back(&child->info);
		
		info.started = true;
	}

	Block res;

	if (is_cancelled)
		return res;

	info.work_stopwatch.start();
	res.swap(readImpl().ref());		/// Трюк, чтобы работало RVO.
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
	{
		info.update(res);

		if (enabled_extremes)
			updateExtremes(res);

		if (!checkLimits())
		{
			res.clear();
			return res;
		}

		if (quota != NULL)
			checkQuota(res);
	}
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

	progress(res.rows(), res.bytes());

	return res;
}


void IProfilingBlockInputStream::readSuffix()
{
	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		(*it)->readSuffix();

	readSuffixImpl();
}


void IProfilingBlockInputStream::updateExtremes(Block & block)
{
	size_t columns = block.columns();

	if (!extremes)
	{
		extremes = block.cloneEmpty();

		for (size_t i = 0; i < columns; ++i)
		{
			Field min_value;
			Field max_value;

			block.getByPosition(i).column->getExtremes(min_value, max_value);

			ColumnPtr & column = extremes.getByPosition(i).column;

			if (column->isConst())
				column = dynamic_cast<const IColumnConst &>(*column).convertToFullColumn();

			column->insert(min_value);
			column->insert(max_value);
		}
	}
	else
	{
		for (size_t i = 0; i < columns; ++i)
		{
			ColumnPtr & column = extremes.getByPosition(i).column;
			
			Field min_value = (*column)[0];
			Field max_value = (*column)[1];

			Field cur_min_value;
			Field cur_max_value;

			block.getByPosition(i).column->getExtremes(cur_min_value, cur_max_value);

			if (cur_min_value < min_value)
				min_value = cur_min_value;
			if (cur_max_value > max_value)
				max_value = cur_max_value;

			column = column->cloneEmpty();
			column->insert(min_value);
			column->insert(max_value);
		}
	}
}


bool IProfilingBlockInputStream::checkLimits()
{
	/// Проверка ограничений.
	if ((limits.max_rows_to_read && info.rows > limits.max_rows_to_read)
		|| (limits.max_bytes_to_read && info.bytes > limits.max_bytes_to_read))
	{
		if (limits.read_overflow_mode == Limits::THROW)
			throw Exception(std::string("Limit for ")
				+ (limits.mode == LIMITS_CURRENT ? "result rows" : "rows to read")
				+ " exceeded: read " + toString(info.rows)
				+ " rows, maximum: " + toString(limits.max_rows_to_read),
				ErrorCodes::TOO_MUCH_ROWS);

		if (limits.read_overflow_mode == Limits::BREAK)
			return false;

		throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	if (limits.max_execution_time != 0
		&& info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
	{
		if (limits.timeout_overflow_mode == Limits::THROW)
			throw Exception("Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
				+ " seconds, maximum: " + toString(limits.max_execution_time.totalMicroseconds() / 1000000.0),
			ErrorCodes::TIMEOUT_EXCEEDED);

		if (limits.timeout_overflow_mode == Limits::BREAK)
			return false;

		throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


void IProfilingBlockInputStream::checkQuota(Block & block)
{
	time_t current_time = time(0);
	double total_elapsed = info.total_stopwatch.elapsedSeconds();

	switch (limits.mode)
	{
		case LIMITS_TOTAL:
			/// Проверяется в методе progress.
			break;

		case LIMITS_CURRENT:
			quota->checkAndAddResultRowsBytes(current_time, block.rows(), block.bytes());
			quota->checkAndAddExecutionTime(current_time, Poco::Timespan((total_elapsed - prev_elapsed) * 1000000.0));
			break;

		default:
			throw Exception("Logical error: unknown limits mode.", ErrorCodes::LOGICAL_ERROR);
	}

	prev_elapsed = total_elapsed;
}


void IProfilingBlockInputStream::progressImpl(size_t rows, size_t bytes)
{
	/// Данные для прогресса берутся из листовых источников.
	if (children.empty())
	{
		if (progress_callback)
			progress_callback(rows, bytes);

		if (process_list_elem)
		{
			process_list_elem->update(rows, bytes);

			/// Общее количество данных, обработанных во всех листовых источниках, возможно, на удалённых серверах.
			
			size_t total_rows = process_list_elem->rows_processed;
			size_t total_bytes = process_list_elem->bytes_processed;
			double total_elapsed = info.total_stopwatch.elapsedSeconds();

			/** Проверяем ограничения на объём данных для чтения, скорость выполнения запроса, квоту на объём данных для чтения.
			  * NOTE: Может быть, имеет смысл сделать, чтобы они проверялись прямо в ProcessList?
			  */

			if (limits.mode == LIMITS_TOTAL
				&& ((limits.max_rows_to_read && total_rows > limits.max_rows_to_read)
					|| (limits.max_bytes_to_read && total_bytes > limits.max_bytes_to_read)))
			{
				if (limits.read_overflow_mode == Limits::THROW)
					throw Exception("Limit for rows to read exceeded: read " + toString(total_rows)
						+ " rows, maximum: " + toString(limits.max_rows_to_read),
						ErrorCodes::TOO_MUCH_ROWS);
				else if (limits.read_overflow_mode == Limits::BREAK)
					cancel();
				else
					throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
			}

			if (limits.min_execution_speed
				&& total_elapsed > limits.timeout_before_checking_execution_speed.totalMicroseconds() / 1000000.0
				&& total_rows / total_elapsed < limits.min_execution_speed)
			{
				throw Exception("Query is executing too slow: " + toString(total_rows / total_elapsed)
					+ " rows/sec., minimum: " + toString(limits.min_execution_speed),
					ErrorCodes::TOO_SLOW);
			}

			if (quota != NULL && limits.mode == LIMITS_TOTAL)
			{
				quota->checkAndAddReadRowsBytes(time(0), rows, bytes);
			}
		}
	}
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


void IProfilingBlockInputStream::setProcessListElement(ProcessList::Element * elem)
{
	process_list_elem = elem;

	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			child->setProcessListElement(elem);
}


const Block & IProfilingBlockInputStream::getTotals()
{
	if (totals)
		return totals;

	for (BlockInputStreams::iterator it = children.begin(); it != children.end(); ++it)
	{
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&**it))
		{
			const Block & res = child->getTotals();
			if (res)
				return res;
		}
	}

	return totals;
}

const Block & IProfilingBlockInputStream::getExtremes() const
{
	if (extremes)
		return extremes;

	for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
	{
		if (const IProfilingBlockInputStream * child = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
		{
			const Block & res = child->getExtremes();
			if (res)
				return res;
		}
	}

	return extremes;
}


}
