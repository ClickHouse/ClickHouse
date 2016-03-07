#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Names.h>
#include <DB/Core/Block.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/ColumnsWithTypeAndName.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ILLEGAL_TYPE_OF_ARGUMENT;
	extern const int ILLEGAL_COLUMN;
	extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
	extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
}

struct ExpressionAction;


/** Интерфейс для обычных функций.
  * Обычные функции - это функции, которые не меняют количество строк в таблице,
  *  и результат работы которых для каждой строчки не зависит от других строк.
  *
  * Функция может принимать произвольное количество аргументов и возвращать произвольное количество значений.
  * Типы и количество значений результата зависят от типов и количества аргументов.
  *
  * Функция диспетчеризуется для целого блока. Это позволяет производить всевозможные проверки редко,
  *  и делать основную работу в виде эффективного цикла.
  *
  * Функция применяется к одному или нескольким столбцам блока, и записывает свой результат,
  *  добавляя новые столбцы к блоку. Функция не модифицирует свои агрументы.
  */
class IFunction
{
public:
	/** Наследник IFunction должен реализовать:
	  * - getName
	  * - либо getReturnType, либо getReturnTypeAndPrerequisites
	  * - одну из перегрузок execute.
	  */

	/// Получить основное имя функции.
	virtual String getName() const = 0;

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	/// Перегрузка для тех, кому не нужны prerequisites и значения константных аргументов. Снаружи не вызывается.
	virtual DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Получить тип результата по типам аргументов и значениям константных аргументов.
	  * Если функция неприменима для данных аргументов - кинуть исключение.
	  * Еще можно вернуть описание дополнительных столбцов, которые требуются для выполнения функции.
	  * Для неконстантных столбцов arguments[i].column = nullptr.
	  * Осмысленные типы элементов в out_prerequisites: APPLY_FUNCTION, ADD_COLUMN.
	  */
	virtual void getReturnTypeAndPrerequisites(
		const ColumnsWithTypeAndName & arguments,
		DataTypePtr & out_return_type,
		std::vector<ExpressionAction> & out_prerequisites)
	{
		DataTypes types(arguments.size());
		for (size_t i = 0; i < arguments.size(); ++i)
			types[i] = arguments[i].type;
		out_return_type = getReturnType(types);
	}

	/// Вызывается, если хоть один агрумент функции - лямбда-выражение.
	/// Для аргументов-лямбда-выражений определяет типы аргументов этих выражений и кладет результат в arguments.
	virtual void getLambdaArgumentTypes(DataTypes & arguments) const
	{
		throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	/// Выполнить функцию над блоком. Замечание: может вызываться одновременно из нескольких потоков, для одного объекта.
	/// Перегрузка для тех, кому не нужны prerequisites. Снаружи не вызывается.
	virtual void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		throw Exception("execute is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/// Выполнить функцию над блоком. Замечание: может вызываться одновременно из нескольких потоков, для одного объекта.
	/// prerequisites идут в том же порядке, что и out_prerequisites, полученные из getReturnTypeAndPrerequisites.
	virtual void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
	{
		execute(block, arguments, result);
	}

	/** Позволяет узнать, является ли функция монотонной в некотором диапазоне значений.
	  * Это используется для работы с индексом в сортированном куске данных.
	  * И позволяет использовать индекс не только, когда написано, например date >= const, но и, например, toMonth(date) >= 11.
	  * Всё это рассматривается только для функций одного аргумента.
	  */
	virtual bool hasInformationAboutMonotonicity() const { return false; }

	/// Свойство монотонности на некотором диапазоне.
	struct Monotonicity
	{
		bool is_monotonic = false;	/// Является ли функция монотонной (неубывающей или невозрастающей).
		bool is_positive = true;	/// true, если функция неубывающая, false, если невозрастающая. Если is_monotonic = false, то не важно.

		Monotonicity(bool is_monotonic_ = false, bool is_positive_ = true)
			: is_monotonic(is_monotonic_), is_positive(is_positive_) {}
	};

	/** Получить информацию о монотонности на отрезке значений. Вызывайте только если hasInformationAboutMonotonicity.
	  * В качестве одного из аргументов может быть передан NULL. Это значит, что соответствующий диапазон неограничен слева или справа.
	  */
	virtual Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const
	{
		throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
	}

	virtual ~IFunction() {}
};


using Poco::SharedPtr;

typedef SharedPtr<IFunction> FunctionPtr;


}
