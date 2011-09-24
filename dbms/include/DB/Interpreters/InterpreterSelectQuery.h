#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


/** Интерпретирует запрос SELECT. Возвращает поток блоков с результатами выполнения запроса.
  */
class InterpreterSelectQuery
{
public:
	InterpreterSelectQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

	BlockInputStreamPtr execute();

	DataTypes getReturnTypes();

private:
	StoragePtr getTable();
	
	/** Пометить часть дерева запроса некоторым part_id.
	  * - для того, чтобы потом можно было вычислить только часть выражения из запроса.
	  */
	void setPartID(ASTPtr ast, unsigned part_id);

	enum PartID
	{
		PART_OTHER 	= 1,
		PART_SELECT = 2,
		PART_WHERE 	= 4,
		PART_GROUP 	= 8,
		PART_HAVING = 16,
		PART_ORDER 	= 32,
		PART_BELOW_AGGREGATE_FUNCTIONS = 64,
		PART_ABOVE_AGGREGATE_FUNCTIONS = 128,
	};


	ASTPtr query_ptr;
	Context context;
	size_t max_block_size;
};


}
