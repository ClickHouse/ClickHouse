#pragma once

namespace DB
{

/// До какой стадии выполнен или нужно выполнить SELECT запрос.
namespace QueryProcessingStage
{
	enum Enum
	{
		Complete 			= 0,	/// Полностью.
		WithMergeableState 	= 1,	/// До стадии, когда результаты обработки на разных серверах можно объединить.
		FetchColumns		= 2,	/// Только прочитать/прочитаны указанные в запросе столбцы.
	};
}

}
