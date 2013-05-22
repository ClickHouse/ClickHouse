#include <algorithm>

#include <DB/DataStreams/LimitBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;

LimitBlockInputStream::LimitBlockInputStream(BlockInputStreamPtr input_, size_t limit_, size_t offset_)
	: limit(limit_), offset(offset_), pos(0)
{
	children.push_back(input_);
}


Block LimitBlockInputStream::readImpl()
{
	Block res;
	size_t rows = 0;
	
	/// pos - сколько строк было прочитано, включая последний прочитанный блок

	if (pos >= offset + limit)
		return res;

	do
	{
		res = children.back()->read();
		if (!res)
			return res;
		rows = res.rows();
		pos += rows;
	} while (pos <= offset);

	/// отдать целый блок
	if (pos >= offset + rows && pos <= offset + limit)
		return res;

	/// отдать кусок блока
	size_t start = std::max(
		static_cast<Int64>(0),
		static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows));

	size_t length = std::min(
		static_cast<Int64>(limit), std::min(
		static_cast<Int64>(pos) - static_cast<Int64>(offset),
		static_cast<Int64>(limit) + static_cast<Int64>(offset) - static_cast<Int64>(pos) + static_cast<Int64>(rows)));

	for (size_t i = 0; i < res.columns(); ++i)
		res.getByPosition(i).column = res.getByPosition(i).column->cut(start, length);

	return res;
}

}

