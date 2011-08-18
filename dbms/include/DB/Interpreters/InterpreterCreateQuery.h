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
	/// Добавляет созданную таблицу в контекст, а также возвращает её.
	StoragePtr execute(ASTPtr query, Context & context);
};


}
