#pragma once

#include <DB/Parsers/formatAST.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Core/Block.h>

namespace DB
{

class StorageDistributed;

/** Запись асинхронная - данные сначала записываются на локальную файловую систему, а потом отправляются на удалённые серверы.
 *  Если Distributed таблица использует более одного шарда, то для того, чтобы поддерживалась запись,
 *  при создании таблицы должен быть указан дополнительный параметр у ENGINE - ключ шардирования.
 *  Ключ шардирования - произвольное выражение от столбцов. Например, rand() или UserID.
 *  При записи блок данных разбивается по остатку от деления ключа шардирования на суммарный вес шардов,
 *  и полученные блоки пишутся в сжатом Native формате в отдельные директории для отправки.
 *  Для каждого адреса назначения (каждой директории с данными для отправки), в StorageDistributed создаётся отдельный поток,
 *  который следит за директорией и отправляет данные. */
class DistributedBlockOutputStream : public IBlockOutputStream
{
public:
	DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast);

	void write(const Block & block) override;

private:
	std::vector<IColumn::Filter> createFilters(Block block);

	void writeSplit(const Block & block);

	void writeImpl(const Block & block, const size_t shard_id = 0);

	void writeToLocal(const Block & block, const size_t repeats);

	void writeToShard(const Block & block, const std::vector<std::string> & dir_names);

private:
	StorageDistributed & storage;
	ASTPtr query_ast;
};

}
