#ifndef DBMS_PARSERS_ASTFUNCTION_H
#define DBMS_PARSERS_ASTFUNCTION_H

#include <DB/Parsers/IAST.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Применение функции или оператора
  */
class ASTFunction : public IAST
{
public:
	StringRange range;
	/// имя функции
	String name;
	/// параметры
	ASTPtr arguments;

	/// сама функция
	FunctionPtr function;
	/// типы возвращаемых значений
	DataTypes return_types;

	ASTFunction() {}
	ASTFunction(StringRange range_) : range(range_) {}

	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }

	/** Получить всех детей. */
	ASTs getChildren()
	{
		ASTs res;
		res.push_back(arguments);
		return res;
	}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Function_" + name; }
};

}

#endif
