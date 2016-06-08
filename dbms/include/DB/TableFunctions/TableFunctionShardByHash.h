#pragma once

#include <DB/TableFunctions/ITableFunction.h>


namespace DB
{

/* shardByHash(cluster, 'key', db, table) - создаёт временный StorageDistributed,
 *  используя кластер cluster, и выбирая из него только один шард путём хэширования строки key.
 *
 * Аналогично функции remote, чтобы получить структуру таблицы, делается запрос DESC TABLE на удалённый сервер.
 */
class TableFunctionShardByHash : public ITableFunction
{
public:
	std::string getName() const override { return "shardByHash"; }
	StoragePtr execute(ASTPtr ast_function, Context & context) const override;
};

}
