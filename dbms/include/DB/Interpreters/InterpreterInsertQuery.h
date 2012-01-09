#pragma once

#include <DB/Interpreters/Context.h>


namespace DB
{


/** Интерпретирует запрос INSERT.
  */
class InterpreterInsertQuery
{
public:
	InterpreterInsertQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

	/** Выполнить запрос.
	  * remaining_data_istr, если не NULL, может содержать нераспарсенные данные для вставки.
	  * (заранее может быть считан в оперативку для парсинга лишь небольшой кусок запроса, который содержит не все данные)
	  */
	void execute(ReadBuffer * remaining_data_istr);

private:
	StoragePtr getTable();
	
	ASTPtr query_ptr;
	Context context;
	size_t max_block_size;
};


}
