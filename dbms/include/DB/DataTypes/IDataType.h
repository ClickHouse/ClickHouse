#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Field.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

/** Метаданные типа для хранения (столбца).
  * Содержит методы для сериализации/десериализации.
  */
class IDataType
{
public:
	/// Основное имя типа (например, UInt64).
	virtual std::string getName() const = 0;

	/// Является ли тип числовым. Дата и дата-с-временем тоже считаются такими.
	virtual bool isNumeric() const { return false; }

	/// Если тип числовой, уместны ли с ним все арифметические операции и приведение типов.
	/// true для чисел, false для даты и даты-с-временем.
	virtual bool behavesAsNumber() const { return false; }

	/// Клонировать
	virtual SharedPtr<IDataType> clone() const = 0;

	/** Бинарная сериализация - для сохранения на диск / в сеть и т. п.
	  * Обратите внимание, что присутствует по два вида методов
	  * - для работы с единичными значениями и целыми столбцами.
	  */
	virtual void serializeBinary(const Field & field, WriteBuffer & ostr) const = 0;
	virtual void deserializeBinary(Field & field, ReadBuffer & istr) const = 0;

	/** Сериализация столбца.
	  * offset и limit используются, чтобы сериализовать часть столбца.
	  * limit = 0 - означает - не ограничено.
	  * offset не должен быть больше размера столбца.
	  * offset + limit может быть больше размера столбца
	  *  - в этом случае, столбец сериализуется до конца.
	  */
	virtual void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const = 0;

	/** Считать не более limit значений и дописать их в конец столбца.
	  * avg_value_size_hint - если не 0, то может использоваться, чтобы избежать реаллокаций при чтении строкового столбца.
	  */
	virtual void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const = 0;

	/** Текстовая сериализация - для вывода на экран / сохранения в текстовый файл и т. п.
	  * Без эскейпинга и квотирования.
	  */
	virtual void serializeText(const Field & field, WriteBuffer & ostr) const = 0;
	virtual void deserializeText(Field & field, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация с эскейпингом, но без квотирования.
	  */
	virtual void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const = 0;
	virtual void deserializeTextEscaped(Field & field, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация в виде литерала, который может быть вставлен в запрос.
	  */
	virtual void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const = 0;
	virtual void deserializeTextQuoted(Field & field, ReadBuffer & istr) const = 0;

	/** Текстовая сериализация в виде литерала для использования в формате JSON.
	  */
	virtual void serializeTextJSON(const Field & field, WriteBuffer & ostr) const = 0;

	/** Создать пустой столбец соответствующего типа.
	  */
	virtual SharedPtr<IColumn> createColumn() const = 0;

	/** Создать столбец соответствующего типа, содержащий константу со значением Field, длины size.
	  */
	virtual SharedPtr<IColumn> createConstColumn(size_t size, const Field & field) const = 0;

	/** Получить значение "по-умолчанию".
	  */
	virtual Field getDefault() const = 0;

	/// Вернуть приблизительный (оценочный) размер значения.
	virtual size_t getSizeOfField() const
	{
		throw Exception("getSizeOfField() method is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	virtual ~IDataType() {}
};


typedef Poco::SharedPtr<IDataType> DataTypePtr;
typedef std::vector<DataTypePtr> DataTypes;

}

