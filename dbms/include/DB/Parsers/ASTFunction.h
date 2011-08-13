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
	/// имя функции
	String name;
	/// аргументы
	ASTPtr arguments;

	/// сама функция
	FunctionPtr function;
	/// типы возвращаемых значений
	DataTypes return_types;
	/// номера столбцов возвращаемых значений
	ColumnNumbers return_column_numbers;

	ASTFunction() {}
	ASTFunction(StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Function_" + name; }
};

}

#endif
