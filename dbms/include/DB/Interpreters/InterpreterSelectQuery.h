#pragma once

#include <DB/Core/QueryProcessingStage.h>
#include <DB/Interpreters/Expression.h>
#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/Parsers/ASTSelectQuery.h>


namespace DB
{


/** Интерпретирует запрос SELECT. Возвращает поток блоков с результатами выполнения запроса до стадии to_stage.
  */
class InterpreterSelectQuery
{
public:
	InterpreterSelectQuery(ASTPtr query_ptr_, Context & context_, QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete);

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

	void setColumns();
	
	/** Пометить часть дерева запроса некоторым part_id.
	  * - для того, чтобы потом можно было вычислить только часть выражения из запроса.
	  */
	void setPartID(ASTPtr ast, unsigned part_id);

	/** Выбрать из списка столбцов какой-нибудь, лучше - минимального размера.
	  */
	String getAnyColumn();

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


	/// Разные стадии выполнения запроса.
	void executeFetchColumns(		BlockInputStreams & streams, ExpressionPtr & expression);
	void executeWhere(				BlockInputStreams & streams, ExpressionPtr & expression);
	void executeAggregation(		BlockInputStreams & streams, ExpressionPtr & expression);
	void executeFinalizeAggregates(	BlockInputStreams & streams, ExpressionPtr & expression);
	void executeHaving(				BlockInputStreams & streams, ExpressionPtr & expression);
	void executeOuterExpression(	BlockInputStreams & streams, ExpressionPtr & expression);
	void executeOrder(				BlockInputStreams & streams, ExpressionPtr & expression);
	void executeUnion(				BlockInputStreams & streams, ExpressionPtr & expression);
	void executeLimit(				BlockInputStreams & streams, ExpressionPtr & expression);
	

	ASTPtr query_ptr;
	ASTSelectQuery & query;
	Context context;
	QueryProcessingStage::Enum to_stage;
};


}
