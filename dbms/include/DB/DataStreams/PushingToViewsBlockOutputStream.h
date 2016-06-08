#pragma once

#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Storages/StorageView.h>


namespace DB
{


/** Записывает данные в указанную таблицу, при этом рекурсивно вызываясь от всех зависимых вьюшек.
  * Если вьюшка не материализованная, то в нее данные не записываются, лишь перенаправляются дальше.
  */
class PushingToViewsBlockOutputStream : public IBlockOutputStream
{
public:
	PushingToViewsBlockOutputStream(String database, String table, const Context & context_, ASTPtr query_ptr_)
		: context(context_), query_ptr(query_ptr_)
	{
		storage = context.getTable(database, table);

		/** TODO Это очень важная строчка. При любой вставке в таблицу один из stream-ов должен владеть lock-ом.
		  * Хотя сейчас любая вставка в таблицу делается через PushingToViewsBlockOutputStream,
		  *  но ясно, что здесь - не лучшее место для этой функциональности.
		  */
		addTableLock(storage->lockStructure(true));

		Dependencies dependencies = context.getDependencies(database, table);
		for (size_t i = 0; i < dependencies.size(); ++i)
		{
			children.push_back(std::make_shared<PushingToViewsBlockOutputStream>(dependencies[i].first, dependencies[i].second, context, ASTPtr()));
			queries.push_back(dynamic_cast<StorageView &>(*context.getTable(dependencies[i].first, dependencies[i].second)).getInnerQuery());
		}

		if (storage->getName() != "View")
			output = storage->write(query_ptr, context.getSettingsRef());
	}

	void write(const Block & block) override
	{
		for (size_t i = 0; i < children.size(); ++i)
		{
			BlockInputStreamPtr from = std::make_shared<OneBlockInputStream>(block);
			InterpreterSelectQuery select(queries[i], context, QueryProcessingStage::Complete, 0, from);
			BlockInputStreamPtr data = std::make_shared<MaterializingBlockInputStream>(select.execute().in);
			copyData(*data, *children[i]);
		}

		if (output)
			output->write(block);
	}

	void flush() override
	{
		if (output)
			output->flush();
	}

	void writePrefix() override
	{
		if (output)
			output->writePrefix();
	}

	void writeSuffix() override
	{
		if (output)
			output->writeSuffix();
	}

private:
	StoragePtr storage;
	BlockOutputStreamPtr output;
	Context context;
	ASTPtr query_ptr;
	std::vector<BlockOutputStreamPtr> children;
	std::vector<ASTPtr> queries;
};


}
