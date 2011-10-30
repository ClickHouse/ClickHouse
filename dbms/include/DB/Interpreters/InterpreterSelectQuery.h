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

	/// Выполнить запрос, получить поток блоков для чтения
	BlockInputStreamPtr execute();

	/** Выполнить запрос, записать результат в нужном формате в buf.
	  * BlockInputStreamPtr возвращается, чтобы можно было потом получить информацию о плане выполнения запроса.
	  */
	BlockInputStreamPtr executeAndFormat(WriteBuffer & buf);

	DataTypes getReturnTypes();
	Block getSampleBlock();

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
		PART_BEFORE_AGGREGATING = 64,	/// Под агрегатной функцией, или в ветке, не содержащей агрегатных функций
		PART_AFTER_AGGREGATING = 128,
	};


	ASTPtr query_ptr;
	Context context;
	size_t max_block_size;
};


}
