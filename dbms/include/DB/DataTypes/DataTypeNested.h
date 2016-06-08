#pragma once

#include <DB/DataTypes/IDataTypeDummy.h>
#include <DB/Core/NamesAndTypes.h>


namespace DB
{

/** Хранит набор пар (имя, тип) для вложенной структуры данных.
  * Используется только при создании таблицы. Во всех остальных случаях не используется, так как раскрывается в набор отдельных столбцов с типами.
  */
class DataTypeNested final : public IDataTypeDummy
{
private:
	/// Имена и типы вложенных массивов.
	NamesAndTypesListPtr nested;

public:
	DataTypeNested(NamesAndTypesListPtr nested_);

	std::string getName() const override;

	DataTypePtr clone() const override
	{
		return std::make_shared<DataTypeNested>(nested);
	}

	const NamesAndTypesListPtr & getNestedTypesList() const { return nested; }

	static std::string concatenateNestedName(const std::string & nested_table_name, const std::string & nested_field_name);
	/// Возвращает префикс имени до первой точки '.'. Или имя без изменений, если точки нет.
	static std::string extractNestedTableName(const std::string & nested_name);
	/// Возвращает суффикс имени после первой точки справа '.'. Или имя без изменений, если точки нет.
	static std::string extractNestedColumnName(const std::string & nested_name);

	/// Создает новый список в котором колонки типа Nested заменены на несколько вида имя_колонки.имя_вложенной_ячейки
	static NamesAndTypesListPtr expandNestedColumns(const NamesAndTypesList & names_and_types);
};

}
