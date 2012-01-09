#pragma once

#include <DB/Interpreters/Context.h>


namespace DB
{


/** Интерпретирует произвольный запрос.
  */
class InterpreterQuery
{
public:
	InterpreterQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

	/** Выполнить запрос.
	  *
	  * ostr - куда писать результат выполнения запроса, если он есть.
	  * 
	  * remaining_data_istr, если не NULL, может содержать нераспарсенный остаток запроса с данными.
	  *  (заранее может быть считан в оперативку для парсинга лишь небольшой кусок запроса, который содержит не все данные)
	  *
	  * В query_plan,
	  *  после выполнения запроса, может быть записан BlockInputStreamPtr,
	  *  использовавшийся при выполнении запроса,
	  *  чтобы можно было получить информацию о том, как выполнялся запрос.
	  */
	void execute(WriteBuffer & ostr, ReadBuffer * remaining_data_istr, BlockInputStreamPtr & query_plan);

private:
	ASTPtr query_ptr;
	Context context;
	size_t max_block_size;
};


}
