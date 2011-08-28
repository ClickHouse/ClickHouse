#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует операции WHERE, HAVING.
  * На вход подаётся поток блоков, в котором в одном из столбцов типа ColumnUInt8 содержатся условия фильтрации.
  * Возвращается поток блоков, в котором содержатся только отфильтрованные строки, а также столбец с условиями фильтрации убран.
  */
class FilterBlockInputStream : public IBlockInputStream
{
public:
	/// filter_column_ - номер столбца с условиями фильтрации
	FilterBlockInputStream(BlockInputStreamPtr input_, size_t filter_column_);
	Block read();

private:
	BlockInputStreamPtr input;
	size_t filter_column;
};

}
