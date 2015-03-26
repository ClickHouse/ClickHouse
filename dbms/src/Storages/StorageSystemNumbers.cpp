#include <Poco/SharedPtr.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/StorageSystemNumbers.h>


namespace DB
{

using Poco::SharedPtr;

class NumbersBlockInputStream : public IProfilingBlockInputStream
{
public:
	NumbersBlockInputStream(size_t block_size_, size_t offset_, size_t step_)
		: block_size(block_size_), next(offset_), step(step_) {}

	String getName() const { return "NumbersBlockInputStream"; }
	String getID() const { return "Numbers"; }

protected:
	Block readImpl()
	{
		Block res;

		ColumnWithNameAndType column_with_name_and_type;

		column_with_name_and_type.name = "number";
		column_with_name_and_type.type = new DataTypeUInt64();
		ColumnUInt64 * column = new ColumnUInt64(block_size);
		ColumnUInt64::Container_t & vec = column->getData();
		column_with_name_and_type.column = column;

		size_t curr = next;		/// Локальная переменная почему-то работает быстрее (>20%), чем член класса.
		UInt64 * pos = &vec[0];	/// Это тоже ускоряет код.
		UInt64 * end = &vec[block_size];
		while (pos < end)
			*pos++ = curr++;

		res.insert(column_with_name_and_type);

		next += step;
		return res;
	}
private:
	size_t block_size;
	UInt64 next;
	UInt64 step;
};


StorageSystemNumbers::StorageSystemNumbers(const std::string & name_, bool multithreaded_)
	: name(name_), columns{{"number", new DataTypeUInt64}}, multithreaded(multithreaded_)
{
}

StoragePtr StorageSystemNumbers::create(const std::string & name_, bool multithreaded_)
{
	return (new StorageSystemNumbers(name_, multithreaded_))->thisPtr();
}


BlockInputStreams StorageSystemNumbers::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	if (!multithreaded)
		threads = 1;

	BlockInputStreams res(threads);
	for (size_t i = 0; i < threads; ++i)
		res[i] = new NumbersBlockInputStream(max_block_size, i * max_block_size, threads * max_block_size);

	return res;
}

}
