#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/** Умеет выбирать куски для слияния и сливать их.
  * Требуется внешний механизм координации слияний со вставками и другими слияниями, обеспечивающий:
  *  - Куски, между которыми еще может появиться новый кусок, нельзя сливать. См. METR-7001.
  *  - Кусок, который уже сливаются с кем-то в одном месте, нельзя начать сливать в кем-то другим в другом месте.
  */
class MergeTreeDataMerger
{
public:
	MergeTreeDataMerger(MergeTreeData & data_) : data(data_), log(&Logger::get("MergeTreeDataMerger")), canceled(false) {}

	/// Если merge_anything_for_old_months, для кусков за прошедшие месяцы снимается ограничение на соотношение размеров.
	bool selectPartsToMerge(
		MergeTreeData::DataPartsVector & what,
		size_t available_disk_space,
		bool merge_anything_for_old_months,
		bool aggressive);

	/// Возвращает название нового куска. Если слияние отменили, возвращает пустую строку.
	String mergeParts(const MergeTreeData::DataPartsVector & parts);

	/// Примерное количество места на диске, нужное для мерджа. С запасом.
	size_t estimateDiskSpaceForMerge(const MergeTreeData::DataPartsVector & parts);

	/** Отменяет все текущие мерджи. Все выполняющиеся сейчас вызовы mergeParts скоро отменят слияние и вернут пустую строку.
	  * После этого с этим экземпляром ничего делать нельзя.
	  */
	void cancelAll() { canceled = true; }

private:
	MergeTreeData & data;

	Logger * log;

	volatile bool canceled;
};

}
