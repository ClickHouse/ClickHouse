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
	PushingToViewsBlockOutputStream(String database_, String table_, const Context &context_, ASTPtr query_ptr_)
		:database(database_), table(table_), context(context_), query_ptr(query_ptr_)
	{
		if (database.empty())
			database = context.getCurrentDatabase();
		storage = context.getTable(database, table);
		Dependencies dependencies = context.getDependencies(DatabaseAndTableName(database, table));
		for (size_t i = 0; i < dependencies.size(); ++i)
		{
			children.push_back(new PushingToViewsBlockOutputStream(dependencies[i].first, dependencies[i].second, context, ASTPtr()));
			queries.push_back(dynamic_cast<StorageView &>(*context.getTable(dependencies[i].first, dependencies[i].second)).getInnerQuery());
		}
	}

	String getName() const { return "PushingToViewsBlockOutputStream"; }

	void write(const Block & block)
	{
		for (size_t i = 0; i < children.size(); ++i)
		{
			BlockInputStreamPtr from = new OneBlockInputStream(block);
			InterpreterSelectQuery select(queries[i], context, QueryProcessingStage::Complete, 0, from);
			BlockInputStreamPtr data = new MaterializingBlockInputStream(select.execute());
			copyData(*data, *children[i]);
		}

		Block buf(block);
		if (storage->getName() != "View")
			storage->write(query_ptr)->write(buf);
	}

private:
	StoragePtr storage;
	String database;
	String table;
	Context context;
	ASTPtr query_ptr;
	std::vector<BlockOutputStreamPtr> children;
	std::vector<ASTPtr> queries;
};


}
