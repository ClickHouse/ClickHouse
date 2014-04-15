#pragma once

#include <DB/Core/QueryProcessingStage.h>
#include <DB/DataStreams/BlockIO.h>
#include <DB/Interpreters/Context.h>


namespace DB
{


/** Интерпретирует произвольный запрос.
  */
class InterpreterQuery
{
public:
	InterpreterQuery(ASTPtr query_ptr_, Context & context_, QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);

	/** Выполнить запрос.
	  *
	  * ostr - куда писать результат выполнения запроса, если он есть.
	  * 
	  * remaining_data_istr, если не nullptr, может содержать нераспарсенный остаток запроса с данными.
	  *  (заранее может быть считан в оперативку для парсинга лишь небольшой кусок запроса, который содержит не все данные)
	  *
	  * В query_plan,
	  *  после выполнения запроса, может быть записан BlockInputStreamPtr,
	  *  использовавшийся при выполнении запроса,
	  *  чтобы можно было получить информацию о том, как выполнялся запрос.
	  */
	void execute(WriteBuffer & ostr, ReadBuffer * remaining_data_istr, BlockInputStreamPtr & query_plan);

	/** Подготовить запрос к выполнению. Вернуть потоки блоков, используя которые можно выполнить запрос.
	  */
	BlockIO execute();

private:
	ASTPtr query_ptr;
	Context context;
	QueryProcessingStage::Enum stage;

	void throwIfReadOnly()
	{
		if (context.getSettingsRef().limits.readonly)
			throw Exception("Cannot execute query in readonly mode", ErrorCodes::READONLY);
	}
};


}
