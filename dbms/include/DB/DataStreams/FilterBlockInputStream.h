#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует операции WHERE, HAVING.
  * На вход подаётся поток блоков, в котором в одном из столбцов типа ColumnUInt8 содержатся условия фильтрации.
  * Возвращается поток блоков, в котором содержатся только отфильтрованные строки.
  */
class FilterBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// filter_column_ - номер столбца с условиями фильтрации.
	FilterBlockInputStream(BlockInputStreamPtr input_, ssize_t filter_column_);
	FilterBlockInputStream(BlockInputStreamPtr input_, const String & filter_column_name_);

	String getName() const { return "FilterBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Filter(" << input->getID() << ", " << filter_column << ", " << filter_column_name << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	IBlockInputStream * input;
	ssize_t filter_column;
	String filter_column_name;
};

}
