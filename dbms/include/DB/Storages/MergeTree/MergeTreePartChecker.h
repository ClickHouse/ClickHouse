#pragma once

#include <DB/Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class MergeTreePartChecker
{
public:
	struct Settings
	{
		bool verbose = false; /// Пишет в stderr прогресс и ошибки, и не останавливается при первой ошибке.
		bool require_checksums = false; /// Требует, чтобы был columns.txt.
		bool require_column_files = false; /// Требует, чтобы для всех столбцов из columns.txt были файлы.
		size_t index_granularity = 8192;

		Settings & setVerbose(bool verbose_) { verbose = verbose_; return *this; }
		Settings & setRequireChecksums(bool require_checksums_) { require_checksums = require_checksums_; return *this; }
		Settings & setRequireColumnFiles(bool require_column_files_) { require_column_files = require_column_files_; return *this; }
		Settings & setIndexGranularity(size_t index_granularity_) { index_granularity = index_granularity_; return *this; }
	};

	/** Полностью проверяет данные кусочка:
	  *  - Вычисляет контрольные суммы и сравнивает с checksums.txt.
	  *  - Для массивов и строк проверяет соответствие размеров и количества данных.
	  *  - Проверяет правильность засечек.
	  * Бросает исключение, если кусок испорчен или если проверить не получилось (TODO: можно попробовать разделить эти случаи).
	  */
	static void checkDataPart(String path, const Settings & settings, const DataTypeFactory & data_type_factory,
							  MergeTreeData::DataPart::Checksums * out_checksums = nullptr);
};

}
