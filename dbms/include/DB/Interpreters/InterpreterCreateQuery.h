#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Позволяет создать новую таблицу или создать объект уже существующей таблицы.
  */
class InterpreterCreateQuery
{
public:
	InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_ = DEFAULT_BLOCK_SIZE);
	
	/// Добавляет созданную таблицу в контекст, а также возвращает её.
	StoragePtr execute();

private:
	ASTPtr query_ptr;
	Context context;
	size_t max_block_size;
};


}
