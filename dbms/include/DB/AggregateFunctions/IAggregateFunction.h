#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Row.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

/** Интерфейс для агрегатных функций.
  * Агрегатные функции аккумулируют значения, которые в них передают, держа в себе некоторое состояние.
  * Поэтому, в отличие от обычных функций, вам необходимо при каждой агрегации, создавать новый экземпляр класса.
  */
class IAggregateFunction
{
public:
	/// Получить основное имя функции.
	virtual String getName() const = 0;

	/// Получить строку, по которой можно потом будет создать объект того же типа (с помощью AggregateFunctionFactory)
	virtual String getTypeID() const = 0;

	/// Создать новую агрегатную функцию того же типа.
	virtual SharedPtr<IAggregateFunction> cloneEmpty() const = 0;

	/** Указать типы аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	  * Необходимо вызывать перед остальными вызовами.
	  */
	virtual void setArguments(const DataTypes & arguments) = 0;

	/// Получить тип результата.
	virtual DataTypePtr getReturnType() const = 0;

	/// Добавить значение.
	virtual void add(const Row & row) = 0;

	/// Объединить состояние с другой агрегатной функцией.
	virtual void merge(const IAggregateFunction & rhs) = 0;

	/// Сериализовать состояние (например, для передачи по сети).
	virtual void serialize(WriteBuffer & buf) const = 0;

	/// Десериализовать состояние и объединить своё состояние с ним.
	virtual void deserializeMerge(ReadBuffer & buf) = 0;

	/// Получить результат
	virtual Field getResult() const = 0;
};


using Poco::SharedPtr;

typedef SharedPtr<IAggregateFunction> AggregateFunctionPtr;
typedef std::vector<AggregateFunctionPtr> AggregateFunctions;

template <> struct TypeName<AggregateFunctionPtr> { static std::string get() { return "AggregateFunctionPtr"; } };
template <> struct NearestFieldType<AggregateFunctionPtr> { typedef AggregateFunctionPtr Type; };

}
