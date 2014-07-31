#pragma once

#include <DB/DataTypes/IDataType.h>

namespace DB
{

/** Знает имена и типы всех возможных виртуальных столбцов.
  * Нужно для движков, перенаправляющих запрос в другие таблицы, не зная заранее, какие в них есть виртуальные столбцы.
  */
class VirtualColumnFactory
{
public:
	static bool hasColumn(const String & name);
	static DataTypePtr getType(const String & name);

	static DataTypePtr tryGetType(const String & name);
};

}
