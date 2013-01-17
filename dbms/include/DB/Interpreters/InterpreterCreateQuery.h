#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Позволяет создать новую таблицу, или создать объект уже существующей таблицы, или создать БД, или создать объект уже существующей БД
  */
class InterpreterCreateQuery
{
public:
	InterpreterCreateQuery(ASTPtr query_ptr_, Context & context_);
	
	/** В случае таблицы: добавляет созданную таблицу в контекст, а также возвращает её.
	  * В случае БД: добавляет созданную БД в контекст и возвращает NULL.
	  * assume_metadata_exists - не проверять наличие файла с метаданными и не создавать его
	  *  (для случая выполнения запроса из существующего файла с метаданными).
	  */
	StoragePtr execute(bool assume_metadata_exists = false);

private:
	ASTPtr query_ptr;
	Context context;
};


}
