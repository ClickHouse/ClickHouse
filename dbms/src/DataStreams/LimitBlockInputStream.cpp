#include <algorithm>

#include <DB/DataStreams/LimitBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;

LimitBlockInputStream::LimitBlockInputStream(SharedPtr<IBlockInputStream> input_, size_t limit_, size_t offset_)
	: input(input_), limit(limit_), offset(offset_), pos(0)
{
}


Block LimitBlockInputStream::read()
{
	Block res;
	size_t rows = 0;

	if (pos >= offset + limit)
		return res;

	while (pos + rows <= offset)
	{
		res = input->read();
		rows = res.rows();
		pos += rows;
	}

	if (pos >= offset && pos + rows <= offset + limit)
	{
		pos += rows;
		return res;
	}

	/// блок, от которого надо выбрать кусок
	for (size_t i = 0; i < res.columns(); ++i)
		res.getByPosition(i).column->cut(std::max(0, static_cast<int>(offset) - static_cast<int>(pos)), limit);
	pos += rows;
	return res;
}

}

