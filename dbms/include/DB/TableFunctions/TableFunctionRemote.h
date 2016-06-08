#pragma once

#include <DB/TableFunctions/ITableFunction.h>


namespace DB
{

/*
 * remote('address', db, table) - создаёт временный StorageDistributed.
 * Чтобы получить структуру таблицы, делается запрос DESC TABLE на удалённый сервер.
 * Например:
 * SELECT count() FROM remote('example01-01-1', merge, hits) - пойти на example01-01-1, в БД merge, таблицу hits.
 * В качестве имени хоста может быть указано также выражение, генерирующее множество шардов и реплик - см. ниже.
 */
class TableFunctionRemote : public ITableFunction
{
public:
	std::string getName() const override { return "remote"; }
	StoragePtr execute(ASTPtr ast_function, Context & context) const override;
};

}
