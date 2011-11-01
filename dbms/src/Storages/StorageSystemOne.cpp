#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/Storages/StorageSystemOne.h>


namespace DB
{


OneValueBlockInputStream::OneValueBlockInputStream() : has_been_read(false)
{
}


Block OneValueBlockInputStream::readImpl()
{
	Block res;
	if (has_been_read)
		return res;

	has_been_read = true;
	ColumnWithNameAndType col;
	col.name = "dummy";
	col.type = new DataTypeUInt8;
	col.column = new ColumnConstUInt8(1, 0);
	res.insert(col);
	return res;
}


StorageSystemOne::StorageSystemOne(const std::string & name_)
	: name(name_)
{
	columns.push_back(NameAndTypePair("dummy", new DataTypeUInt8));
}


BlockInputStreamPtr StorageSystemOne::read(
	const Names & column_names, ASTPtr query, size_t max_block_size)
{
	check(column_names);
	return new OneValueBlockInputStream();
}


}
