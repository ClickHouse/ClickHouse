#pragma once

#include <DB/Core/QueryProcessingStage.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/TableFunctions/ITableFunction.h>

namespace DB
{


/** Интерпретирует запрос SELECT. Возвращает поток блоков с результатами выполнения запроса до стадии to_stage.
  */
class InterpreterSelectQuery
{
public:
	InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_, QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete, size_t subquery_depth_ = 0, BlockInputStreamPtr input = NULL);

	InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_, const Names & required_column_names,
		QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete, size_t subquery_depth_ = 0, BlockInputStreamPtr input = NULL);

	/// Выполнить запрос, получить поток блоков для чтения
	BlockInputStreamPtr execute();

	/** Выполнить запрос, записать результат в нужном формате в buf.
	  * BlockInputStreamPtr возвращается, чтобы можно было потом получить информацию о плане выполнения запроса.
	  */
	BlockInputStreamPtr executeAndFormat(WriteBuffer & buf);

	DataTypes getReturnTypes();
	Block getSampleBlock();

	/** Получить CREATE запрос для таблицы, из которой идёт выбор.
	  */
	ASTPtr getCreateQuery();

private:
	typedef Poco::SharedPtr<ExpressionAnalyzer> ExpressionAnalyzerPtr;
	
	void init(BlockInputStreamPtr input);

	/** Из какой таблицы читать. JOIN-ы не поддерживаются.
	  */
	void getDatabaseAndTableNames(String & database_name, String & table_name);
	
	StoragePtr getTable();

	/** Выбрать из списка столбцов какой-нибудь, лучше - минимального размера.
	  */
	String getAnyColumn();

	/// Разные стадии выполнения запроса.

	/// Вынимает данные из таблицы. Возвращает стадию, до которой запрос был обработан в Storage.
	QueryProcessingStage::Enum executeFetchColumns(BlockInputStreams & streams);

	void executeWhere(				BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeAggregation(		BlockInputStreams & streams, ExpressionActionsPtr expression, bool final);
	void executeMergeAggregated(	BlockInputStreams & streams);
	void executeHaving(				BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeOuterExpression(	BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeOrder(				BlockInputStreams & streams);
	void executePreLimit(			BlockInputStreams & streams);
	void executeUnion(				BlockInputStreams & streams);
	void executeLimit(				BlockInputStreams & streams);
	void executeProjection(			BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeDistinct(			BlockInputStreams & streams, bool before_order);
	

	ASTPtr query_ptr;
	ASTSelectQuery & query;
	Context context;
	Settings settings;
	QueryProcessingStage::Enum to_stage;
	size_t subquery_depth;
	ExpressionAnalyzerPtr query_analyzer;
	BlockInputStreams streams;
	StoragePtr table_function_storage;

	Logger * log;
};


}
