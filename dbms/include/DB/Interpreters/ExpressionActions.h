#pragma once

#include <DB/Functions/IFunction.h>


namespace DB
{
	
typedef std::pair<std::string, std::string> NameWithAlias;
typedef std::vector<NameWithAlias> NamesWithAliases;
	
/** Содержит последовательность действий над блоком.
  */
class ExpressionActions : private boost::noncopyable
{
public:
	struct Action
	{
		enum Type
		{
			APPLY_FUNCTION,
			ADD_COLUMN,
			REMOVE_COLUMN,
			COPY_COLUMN,
		};
		
		Type type;
		
		std::string source_name;
		std::string result_name;
		DataTypePtr result_type;
		
		/// Для ADD_CONST_COLUMN.
		ColumnPtr added_column;
		
		/// Для APPLY_FUNCTION.
		FunctionPtr function;
		Names argument_names;
		
		Action(FunctionPtr function_, const std::vector<std::string> & argument_names_, const std::string & result_name_)
			: type(APPLY_FUNCTION), result_name(result_name_), function(function_), argument_names(argument_names_) {}
			
		explicit Action(ColumnWithNameAndType added_column_)
			: type(ADD_COLUMN), result_name(added_column_.name),
			result_type(added_column_.type), added_column(added_column_.column) {}
			
		Action(const std::string & removed_name)
			: type(REMOVE_COLUMN), source_name(removed_name) {}
			
		Action(const std::string & from_name, const std::string & to_name)
			: type(COPY_COLUMN), source_name(from_name), result_name(to_name) {}
		
		void prepare(Block & sample_block);
		void execute(Block & block);
		
		std::string toString() const;
	};
	
	typedef std::vector<Action> Actions;
	
	ExpressionActions(const NamesAndTypesList & input_columns_)
		: input_columns(input_columns_)
	{
		for (NamesAndTypesList::iterator it = input_columns.begin(); it != input_columns.end(); ++it)
		{
			sample_block.insert(ColumnWithNameAndType(it->second->createConstColumn(1, it->second->getDefault()), it->second, it->first));
		}
	}
	
	void add(const Action & action)
	{
		if (sample_block.has(action.result_name))
			return;
		actions.push_back(action);
		actions.back().prepare(sample_block);
	}
	
	/// - Добавляет действия для удаления лишних столбцов и переименования выходных столбцов в алиасы.
	/// - Убирает входные столбцы, не нужные для получения выходных.
	void finalize(const NamesWithAliases & output_columns);
	
	/// Получить список входных столбцов.
	NamesAndTypesList getRequiredColumns() { return input_columns; }

	/// Выполнить выражение над блоком. Блок должен содержать все столбцы , возвращаемые getRequiredColumns.
	void execute(Block & block);

	/// Получить блок-образец, содержащий имена и типы столбцов результата.
	const Block & getSampleBlock() { return sample_block; }
	
	std::string dumpActions() const;

private:
	NamesAndTypesList input_columns;
	Actions actions;
	Block sample_block;
};

typedef SharedPtr<ExpressionActions> ExpressionActionsPtr;


}
