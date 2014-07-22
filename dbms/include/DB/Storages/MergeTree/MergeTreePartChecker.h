#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class MergeTreePartChecker
{
public:
	/** Полностью проверяет данные кусочка:
	  *  - Вычисляет контрольные суммы и сравнивает с checksums.txt.
	  *  - Для массивов и строк проверяет соответствие размеров и количества данных.
	  *  - Проверяет правильность засечек.
	  * Бросает исключение, если кусок испорчен или если проверить не получилось (TODO: можно попробовать разделить эти случаи).
	  */
	static void checkDataPart(String path, size_t index_granularity, const DataTypeFactory & data_type_factory);
};

}
