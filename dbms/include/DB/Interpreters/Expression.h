#pragma once

#include <set>

#include <Poco/SharedPtr.h>

#include <DB/Parsers/IAST.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Aggregator.h>


namespace DB
{

/** Интерпретирует выражение из синтаксического дерева,
  *  в котором присутствуют имена столбцов, константы и обычные функции.
  */
class Expression : private boost::noncopyable
{
public:
	Expression(const ASTPtr & ast_, const Context & context_) : ast(ast_), context(context_)
	{
		createAliasesDict(ast);
		addSemantic(ast);
		glueTree(ast);
	}

	/** Получить список столбцов, которых необходимо прочитать из таблицы, чтобы выполнить выражение.
	  */
	Names getRequiredColumns();

	/** Преобразовать подзапросы или перечисления значений в секции IN в множества.
	  * Выполняет подзапросы, если требуется.
	  * Заменяет узлы ASTSubquery на узлы ASTSet.
	  * Следует вызывать перед execute, если в выражении могут быть множества.
	  */
	void makeSets(size_t subquery_depth = 0);

	/** Выполнить подзапросы не в секциях IN и FROM и преобразовать их в константы.
	  * Поддерживаются только независимые подзапросы.
	  * Следует вызывать перед execute, если в выражении могут быть скалярные подзапросы.
	  * Заменяет узлы ASTSubquery на узлы ASTLiteral или tuple.
	  */
	void resolveScalarSubqueries(size_t subquery_depth = 0);

	/** Выполнить выражение над блоком. Блок должен содержать все столбцы - идентификаторы.
	  * Функция добавляет в блок новые столбцы - результаты вычислений.
	  * part_id - какую часть выражения вычислять.
	  * Если указано only_consts - то вычисляются только выражения, зависящие от констант.
	  * Если указано clear_temporaries - удалить временные столбцы из блока, которые больше не понадобятся ни для каких вычислений.
	  */
	void execute(Block & block, unsigned part_id = 0, bool only_consts = false);

	/** Убрать из блока столбцы, которые больше не нужны.
	  */
	void clearTemporaries(Block & block);

	/** Взять из блока с промежуточными результатами вычислений только столбцы, представляющие собой конечный результат.
	  * Переименовать их в алиасы, если они заданы и если параметр without_duplicates_and_aliases = false.
	  * Вернуть новый блок, в котором эти столбцы расположены в правильном порядке.
	  */
	Block projectResult(Block & block, bool without_duplicates_and_aliases = false, unsigned part_id = 0, ASTPtr subtree = NULL);

	/** Получить список типов столбцов результата.
	  */
	DataTypes getReturnTypes();

	/** Получить блок-образец, содержащий имена и типы столбцов результата.
	  */
	Block getSampleBlock();

	/** Получить список ключей агрегирования и описаний агрегатных функций, если в запросе есть GROUP BY.
	  */
	void getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates);

	/** Есть ли в выражении агрегатные функции.
	  */
	bool hasAggregates();

	/** Пометить то, что должно быть вычислено до агрегирования одним part_id,
	  * а то, что должно быть вычислено после агрегирования, а также сами агрегатные функции - другим part_id.
	  */
	void markBeforeAggregation(unsigned before_part_id);

	/** Получить информацию об операции arrayJoin, если она есть.
	  * Если есть - в column_name будет записано имя столбца, находящегося внутри arrayJoin.
	  */
	bool getArrayJoinInfo(String & column_name);
	
	/** Пометить то, что должно быть вычислено до применения операции arrayJoin.
	  */
	void markBeforeArrayJoin(unsigned part_id);

private:
	ASTPtr ast;
	const Context context;

	typedef std::set<String> NamesSet;
	NamesSet required_columns;

	typedef std::map<String, ASTPtr> Aliases;
	Aliases aliases;


	NamesAndTypesList::const_iterator findColumn(const String & name);

	/** Создать словарь алиасов.
	  */
	void createAliasesDict(ASTPtr & ast);
		
	/** Для узлов - звёздочек - раскрыть их в список всех столбцов.
	  * Для узлов - литералов - прописать их типы данных.
	  * Для узлов - функций - прописать ссылки на функции, заменить имена на канонические, прописать и проверить типы.
	  * Для узлов - идентификаторов - прописать ссылки на их типы.
	  * Проверить, что все функции применимы для типов их аргументов.
	  */
	void addSemantic(ASTPtr & ast);

	/** Склеить одинаковые узлы в синтаксическом дереве (превращая его в направленный ациклический граф).
	  * Это означает, в том числе то, что функции с одними и теми же аргументами, будут выполняться только один раз.
	  * Например, выражение rand(), rand() вернёт два идентичных столбца.
	  * Поэтому, все функции, в которых такое поведение нежелательно, должны содержать дополнительный параметр "тег".
	  */
	void glueTree(ASTPtr ast);

	typedef std::map<String, ASTPtr> Subtrees;
	
	void glueTreeImpl(ASTPtr ast, Subtrees & Subtrees);

	void executeImpl(ASTPtr ast, Block & block, unsigned part_id, bool only_consts);

	void collectFinalColumns(ASTPtr ast, Block & src, Block & dst, bool without_duplicates, unsigned part_id);

	void getReturnTypesImpl(ASTPtr ast, DataTypes & res);

	void getSampleBlockImpl(ASTPtr ast, Block & res);

	void getAggregateInfoImpl(ASTPtr ast, Names & key_names, AggregateDescriptions & aggregates, NamesSet & processed);

	bool hasAggregatesImpl(ASTPtr ast);

	void markBeforeAggregationImpl(ASTPtr ast, unsigned before_part_id, bool below = false);

	void makeSetsImpl(ASTPtr ast, size_t subquery_depth);

	void resolveScalarSubqueriesImpl(ASTPtr & ast, size_t subquery_depth);

	bool getArrayJoinInfoImpl(ASTPtr ast, String & column_name);

	void markBeforeArrayJoinImpl(ASTPtr ast, unsigned part_id, bool below = false);

	typedef std::set<std::string> NeedColumns;

	void clearTemporariesImpl(ASTPtr ast, Block & block);

	void collectNeedColumns(ASTPtr ast, Block & block, NeedColumns & need_columns, bool top_level = true, bool all_children_need = false);

	/// Получить тип у функции, идентификатора или литерала.
	DataTypePtr getType(ASTPtr ast);
};

typedef SharedPtr<Expression> ExpressionPtr;


}
