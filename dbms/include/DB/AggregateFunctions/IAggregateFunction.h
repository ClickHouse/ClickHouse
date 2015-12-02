#pragma once

#include <memory>

#include <Poco/SharedPtr.h>

#include <DB/Core/Row.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/Common/typeid_cast.h>


namespace DB
{


typedef char * AggregateDataPtr;
typedef const char * ConstAggregateDataPtr;


/** Интерфейс для агрегатных функций.
  * Экземпляры классов с этим интерфейсом не содержат самих данных для агрегации,
  *  а содержат лишь метаданные (описание) агрегатной функции,
  *  а также методы для создания, удаления и работы с данными.
  * Данные, получающиеся во время агрегации (промежуточные состояния вычислений), хранятся в других объектах
  *  (которые могут быть созданы в каком-нибудь пуле),
  *  а IAggregateFunction - внешний интерфейс для манипулирования ими.
  */
class IAggregateFunction
{
public:
	/// Получить основное имя функции.
	virtual String getName() const = 0;

	/** Указать типы аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	  * Необходимо вызывать перед остальными вызовами.
	  */
	virtual void setArguments(const DataTypes & arguments) = 0;

	/** Указать параметры - для параметрических агрегатных функций.
	  * Если параметры не предусмотрены или переданные параметры недопустимы - кинуть исключение.
	  * Если параметры есть - необходимо вызывать перед остальными вызовами, иначе - не вызывать.
	  */
	virtual void setParameters(const Array & params)
	{
		throw Exception("Aggregate function " + getName() + " doesn't allow parameters.",
			ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
	}

	/// Получить тип результата.
	virtual DataTypePtr getReturnType() const = 0;

	virtual ~IAggregateFunction() {};


	/** Функции по работе с данными. */

	/** Создать пустые данные для агрегации с помощью placement new в заданном месте.
	  * Вы должны будете уничтожить их с помощью метода destroy.
	  */
	virtual void create(AggregateDataPtr place) const = 0;

	/// Уничтожить данные для агрегации.
	virtual void destroy(AggregateDataPtr place) const noexcept = 0;

	/// Уничтожать данные не обязательно.
	virtual bool hasTrivialDestructor() const = 0;

	/// Получить sizeof структуры с данными.
	virtual size_t sizeOfData() const = 0;

	/// Как должна быть выровнена структура с данными. NOTE: Сейчас не используется (структуры с состоянием агрегации кладутся без выравнивания).
	virtual size_t alignOfData() const = 0;

	/// Добавить значение. columns - столбцы, содержащие аргументы, row_num - номер строки в столбцах.
	virtual void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const = 0;

	/// Объединить состояние с другим состоянием.
	virtual void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const = 0;

	/// Сериализовать состояние (например, для передачи по сети).
	virtual void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const = 0;

	/// Десериализовать состояние и объединить своё состояние с ним.
	virtual void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const = 0;

	/// Вставить результат в столбец.
	virtual void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const = 0;

	/** Возвращает true для агрегатных функций типа -State.
	  * Они выполняются как другие агрегатные функции, но не финализируются (возвращают состояние агрегации, которое может быть объединено с другим).
	  */
	virtual bool isState() const { return false; }


	/** Внутренний цикл, использующий указатель на функцию, получается лучше, чем использующий виртуальную функцию.
	  * Причина в том, что в случае виртуальных функций, GCC 5.1.2 генерирует код,
	  *  который на каждой итерации цикла заново грузит из памяти в регистр адрес функции (значение по смещению в таблице виртуальных функций).
	  * Это даёт падение производительности на простых запросах в районе 12%.
	  * После появления более хороших компиляторов, код можно будет убрать.
	  */
	using AddFunc = void (*)(const IAggregateFunction *, AggregateDataPtr, const IColumn **, size_t);
	virtual AddFunc getAddressOfAddFunction() const = 0;
};


/// Реализует несколько методов. T - тип структуры с данными для агрегации.
template <typename T>
class IAggregateFunctionHelper : public IAggregateFunction
{
protected:
	typedef T Data;

	static Data & data(AggregateDataPtr place) 				{ return *reinterpret_cast<Data*>(place); }
	static const Data & data(ConstAggregateDataPtr place) 	{ return *reinterpret_cast<const Data*>(place); }

public:
	void create(AggregateDataPtr place) const override
	{
		new (place) Data;
	}

	void destroy(AggregateDataPtr place) const noexcept override
	{
		data(place).~Data();
	}

	bool hasTrivialDestructor() const override
	{
		return __has_trivial_destructor(Data);
	}

	size_t sizeOfData() const override
	{
		return sizeof(Data);
	}

	/// NOTE: Сейчас не используется (структуры с состоянием агрегации кладутся без выравнивания).
	size_t alignOfData() const override
	{
		return __alignof__(Data);
	}
};


using Poco::SharedPtr;

typedef SharedPtr<IAggregateFunction> AggregateFunctionPtr;

}
