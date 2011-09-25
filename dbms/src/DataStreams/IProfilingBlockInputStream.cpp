#include <iomanip>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


void BlockStreamProfileInfo::update(Block & block)
{
	++blocks;
	rows += block.rows();
	for (size_t i = 0; i < block.columns(); ++i)
		bytes += block.getByPosition(i).column->byteSize();

	if (column_names.empty())
		column_names = block.dumpNames();
}


void BlockStreamProfileInfo::print(std::ostream & ostr) const
{
	ostr << std::fixed << std::setprecision(2)
		<< "Columns: " << column_names << std::endl
		<< "Elapsed: " << work_stopwatch.elapsed() / 1000000.0 << " sec. "
		<< "(" << work_stopwatch.elapsed() * 100.0 / total_stopwatch.elapsed() << "%), " << std::endl
		<< "Rows: " << rows << ", per second: " << rows * 1000000 / work_stopwatch.elapsed() << ", " << std::endl
		<< "Blocks: " << blocks << ", per second: " << blocks * 1000000.0 / work_stopwatch.elapsed() << ", " << std::endl
		<< bytes / 1000000.0 << " MB (memory), " << bytes / work_stopwatch.elapsed() << " MB/s (memory), " << std::endl
		<< "Average block size: " << rows / blocks << "." << std::endl;
}

	
Block IProfilingBlockInputStream::read()
{
	if (!info.started)
		info.total_stopwatch.start();
	
	info.work_stopwatch.start();
	Block res = readImpl();
	info.work_stopwatch.stop();

	if (res)
		info.update(res);

/*	if (res)
	{
		std::cerr << std::endl;
		std::cerr << getName() << std::endl;
		getInfo().print(std::cerr);
	}*/

	return res;
}
	

const BlockStreamProfileInfo & IProfilingBlockInputStream::getInfo() const
{
	return info;
}


}
