#pragma once

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

	/** Выполнить выражение над блоком. Блок должен содержать все столбцы - идентификаторы.
	  * Функция добавляет в блок новые столбцы - результаты вычислений.
	  */
	void execute(Block & block);

private:
	ASTPtr ast;
	const Context & context;

	
	/** Для узлов - литералов - прописать их типы данных.
	  * Для узлов - функций - прописать ссылки на функции и заменить имена на канонические.
	  * Для узлов - идентификаторов - прописать ссылки на их типы.
	  */
	void addSemantic(ASTPtr ast);

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
	void setNotCalculated(ASTPtr ast);

	void executeImpl(ASTPtr ast, Block & block);
};


}
