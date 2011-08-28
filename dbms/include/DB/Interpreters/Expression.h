#pragma once

#include <set>

#include <Poco/SharedPtr.h>

#include <DB/Parsers/IAST.h>
#include <DB/Interpreters/Context.h>


namespace DB
{

/** Интерпретирует выражение из синтаксического дерева,
  *  в котором присутствуют имена столбцов, константы и обычные функции.
  */
class Expression
{
public:
	Expression(ASTPtr ast_, const Context & context_) : ast(ast_), context(context_)
	{
		addSemantic(ast);
		checkTypes(ast);
		glueTree(ast);
	}

	/** Получить список столбцов, которых необходимо прочитать из таблицы, чтобы выполнить выражение.
	  */
	Names getRequiredColumns();

	/** Выполнить выражение над блоком. Блок должен содержать все столбцы - идентификаторы.
	  * Функция добавляет в блок новые столбцы - результаты вычислений.
	  * part_id - какую часть выражения вычислять.
	  */
	void execute(Block & block, unsigned part_id = 0);

	/** Взять из блока с промежуточными результатами вычислений только столбцы, представляющие собой конечный результат.
	  * Вернуть новый блок, в котором эти столбцы расположены в правильном порядке.
	  */
	Block projectResult(Block & block, unsigned part_id = 0);

	/** Получить список типов столбцов результата.
	  */
	DataTypes getReturnTypes();

private:
	ASTPtr ast;
	const Context & context;

	typedef std::set<String> NamesSet;
	NamesSet required_columns;

	
	/** Для узлов - звёздочек - раскрыть их в список всех столбцов.
	  * Для узлов - литералов - прописать их типы данных.
	  * Для узлов - функций - прописать ссылки на функции и заменить имена на канонические.
	  * Для узлов - идентификаторов - прописать ссылки на их типы.
	  */
	void addSemantic(ASTPtr & ast);

	/** Проверить, что все функции применимы.
	  */
	void checkTypes(ASTPtr ast);

	/** Склеить одинаковые узлы в синтаксическом дереве (превращая его в направленный ациклический граф).
	  * Это означает, в том числе то, что функции с одними и теми же аргументами, будут выполняться только один раз.
	  * Например, выражение rand(), rand() вернёт два идентичных столбца.
	  * Поэтому, все функции, в которых такое поведение нежелательно, должны содержать дополнительный параметр "тег".
	  */
	void glueTree(ASTPtr ast);

	typedef std::map<String, ASTPtr> Subtrees;
	
	void glueTreeImpl(ASTPtr ast, Subtrees & Subtrees);

	/** Прописать во всех узлах, что они ещё не вычислены.
	  */
	void setNotCalculated(ASTPtr ast, unsigned part_id);

	void executeImpl(ASTPtr ast, Block & block, unsigned part_id);

	void collectFinalColumns(ASTPtr ast, Block & src, Block & dst, unsigned part_id);

	void getReturnTypesImpl(ASTPtr ast, DataTypes & res);
};


}
