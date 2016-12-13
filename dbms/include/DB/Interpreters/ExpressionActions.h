#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Core/Names.h>
#include <DB/Core/ColumnWithTypeAndName.h>
#include <DB/Core/Block.h>
#include <DB/Functions/IFunction.h>

#include <unordered_set>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

using NameWithAlias = std::pair<std::string, std::string>;
using NamesWithAliases = std::vector<NameWithAlias>;

class Join;


/** Действие над блоком.
  */
struct ExpressionAction
{
public:
	enum Type
	{
		ADD_COLUMN,
		REMOVE_COLUMN,
		COPY_COLUMN,

		APPLY_FUNCTION,

		/** Заменяет указанные столбцы с массивами на столбцы с элементами.
			* Размножает значения в остальных столбцах по количеству элементов в массивах.
			* Массивы должны быть параллельными (иметь одинаковые длины).
			*/
		ARRAY_JOIN,

		/// INNER|LEFT JOIN.
		JOIN,

		/// Переупорядочить и переименовать столбцы, удалить лишние. Допускаются одинаковые имена столбцов в результате.
		PROJECT,
	};

	Type type;

	/// For ADD/REMOVE/COPY_COLUMN.
	std::string source_name;
	std::string result_name;
	DataTypePtr result_type;

	/// For ADD_COLUMN.
	ColumnPtr added_column;

	/// For APPLY_FUNCTION.
	mutable FunctionPtr function; /// mutable - чтобы можно было делать execute.
	Names argument_names;
	Names prerequisite_names;

	/// For ARRAY_JOIN
	NameSet array_joined_columns;
	bool array_join_is_left = false;

	/// For JOIN
	std::shared_ptr<const Join> join;
	NamesAndTypesList columns_added_by_join;

	/// For PROJECT.
	NamesWithAliases projection;

	/// Если result_name_ == "", в качестве имени используется "имя_функции(аргументы через запятую)".
	static ExpressionAction applyFunction(
		const FunctionPtr & function_, const std::vector<std::string> & argument_names_, std::string result_name_ = "");

	static ExpressionAction addColumn(const ColumnWithTypeAndName & added_column_)
	{
		ExpressionAction a;
		a.type = ADD_COLUMN;
		a.result_name = added_column_.name;
		a.result_type = added_column_.type;
		a.added_column = added_column_.column;
		return a;
	}

	static ExpressionAction removeColumn(const std::string & removed_name)
	{
		ExpressionAction a;
		a.type = REMOVE_COLUMN;
		a.source_name = removed_name;
		return a;
	}

	static ExpressionAction copyColumn(const std::string & from_name, const std::string & to_name)
	{
		ExpressionAction a;
		a.type = COPY_COLUMN;
		a.source_name = from_name;
		a.result_name = to_name;
		return a;
	}

	static ExpressionAction project(const NamesWithAliases & projected_columns_)
	{
		ExpressionAction a;
		a.type = PROJECT;
		a.projection = projected_columns_;
		return a;
	}

	static ExpressionAction project(const Names & projected_columns_)
	{
		ExpressionAction a;
		a.type = PROJECT;
		a.projection.resize(projected_columns_.size());
		for (size_t i = 0; i < projected_columns_.size(); ++i)
			a.projection[i] = NameWithAlias(projected_columns_[i], "");
		return a;
	}

	static ExpressionAction arrayJoin(const NameSet & array_joined_columns, bool array_join_is_left)
	{
		if (array_joined_columns.empty())
			throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);
		ExpressionAction a;
		a.type = ARRAY_JOIN;
		a.array_joined_columns = array_joined_columns;
		a.array_join_is_left = array_join_is_left;
		return a;
	}

	static ExpressionAction ordinaryJoin(std::shared_ptr<const Join> join_, const NamesAndTypesList & columns_added_by_join_)
	{
		ExpressionAction a;
		a.type = JOIN;
		a.join = join_;
		a.columns_added_by_join = columns_added_by_join_;
		return a;
	}

	/// Какие столбцы нужны, чтобы выполнить это действие.
	/// Если этот Action еще не добавлен в ExpressionActions, возвращаемый список может быть неполным, потому что не учтены prerequisites.
	Names getNeededColumns() const;

	std::string toString() const;

private:
	friend class ExpressionActions;

	std::vector<ExpressionAction> getPrerequisites(Block & sample_block);
	void prepare(Block & sample_block);
	void execute(Block & block) const;
	void executeOnTotals(Block & block) const;
};


/** Содержит последовательность действий над блоком.
  */
class ExpressionActions
{
public:
	using Actions = std::vector<ExpressionAction>;

	ExpressionActions(const NamesAndTypesList & input_columns_, const Settings & settings_)
		: input_columns(input_columns_), settings(settings_)
	{
		for (const auto & input_elem : input_columns)
			sample_block.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));
	}

	/// Для константных столбцов в input_columns_ могут содержаться сами столбцы.
	ExpressionActions(const ColumnsWithTypeAndName & input_columns_, const Settings & settings_)
		: settings(settings_)
	{
		for (const auto & input_elem : input_columns_)
		{
			input_columns.emplace_back(input_elem.name, input_elem.type);
			sample_block.insert(input_elem);
		}
	}

	/// Добавить входной столбец.
	/// Название столбца не должно совпадать с названиями промежуточных столбцов, возникающих при вычислении выражения.
	/// В выражении не должно быть действий PROJECT.
	void addInput(const ColumnWithTypeAndName & column);
	void addInput(const NameAndTypePair & column);

	void add(const ExpressionAction & action);

	/// Кладет в out_new_columns названия новых столбцов
	///  (образовавшихся в результате добавляемого действия и его rerequisites).
	void add(const ExpressionAction & action, Names & out_new_columns);

	/// Добавляет в начало удаление всех лишних столбцов.
	void prependProjectInput();

	/// Добавить в начало указанное действие типа ARRAY JOIN. Поменять соответствующие входные типы на массивы.
	/// Если в списке ARRAY JOIN есть неизвестные столбцы, взять их типы из sample_block, а сразу после ARRAY JOIN удалить.
	void prependArrayJoin(const ExpressionAction & action, const Block & sample_block);

	/// Если последнее действие - ARRAY JOIN, и оно не влияет на столбцы из required_columns, выбросить и вернуть его.
	/// Поменять соответствующие выходные типы на массивы.
	bool popUnusedArrayJoin(const Names & required_columns, ExpressionAction & out_action);

	/// - Добавляет действия для удаления всех столбцов, кроме указанных.
	/// - Убирает неиспользуемые входные столбцы.
	/// - Может как-нибудь оптимизировать выражение.
	/// - Не переупорядочивает столбцы.
	/// - Не удаляет "неожиданные" столбцы (например, добавленные функциями).
	/// - Если output_columns пуст, оставляет один произвольный столбец (чтобы не потерялось количество строк в блоке).
	void finalize(const Names & output_columns);

	/// Получить список входных столбцов.
	Names getRequiredColumns() const
	{
		Names names;
		for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
			names.push_back(it->name);
		return names;
	}

	const NamesAndTypesList & getRequiredColumnsWithTypes() const { return input_columns; }

	/// Выполнить выражение над блоком. Блок должен содержать все столбцы , возвращаемые getRequiredColumns.
	void execute(Block & block) const;

	/** Выполнить выражение над блоком тотальных значений.
	  * Почти не отличается от execute. Разница лишь при выполнении JOIN-а.
	  */
	void executeOnTotals(Block & block) const;

	/// Получить блок-образец, содержащий имена и типы столбцов результата.
	const Block & getSampleBlock() const { return sample_block; }

	std::string getID() const;

	std::string dumpActions() const;

	static std::string getSmallestColumn(const NamesAndTypesList & columns);

	BlockInputStreamPtr createStreamWithNonJoinedDataIfFullOrRightJoin(size_t max_block_size) const;

private:
	NamesAndTypesList input_columns;
	Actions actions;
	Block sample_block;
	Settings settings;

	void checkLimits(Block & block) const;

	/// Добавляет сначала все prerequisites, потом само действие.
	/// current_names - столбцы, prerequisites которых сейчас обрабатываются.
	void addImpl(ExpressionAction action, NameSet & current_names, Names & new_names);

	/// Попробовать что-нибудь улучшить, не меняя списки входных и выходных столбцов.
	void optimize();
	/// Переместить все arrayJoin как можно ближе к концу.
	void optimizeArrayJoin();
};

using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;


/** Последовательность преобразований над блоком.
  * Предполагается, что результат каждого шага подается на вход следующего шага.
  * Используется для выполнения некоторых частей запроса по отдельности.
  *
  * Например, можно составить цепочку из двух шагов:
  * 	1) вычислить выражение в секции WHERE,
  * 	2) вычислить выражение в секции SELECT,
  *  и между двумя шагами делать фильтрацию по значению в секции WHERE.
  */
struct ExpressionActionsChain
{
	struct Step
	{
		ExpressionActionsPtr actions;
		Names required_output;

		Step(ExpressionActionsPtr actions_ = nullptr, Names required_output_ = Names())
			: actions(actions_), required_output(required_output_) {}
	};

	using Steps = std::vector<Step>;

	Settings settings;
	Steps steps;

	void addStep();

	void finalize();

	void clear()
	{
		steps.clear();
	}

	ExpressionActionsPtr getLastActions()
	{
		if (steps.empty())
			throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

		return steps.back().actions;
	}

	Step & getLastStep()
	{
		if (steps.empty())
			throw Exception("Empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

		return steps.back();
	}

	std::string dumpChain();
};

}
