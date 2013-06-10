#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <set>

namespace DB
{
	
ExpressionActions::Action ExpressionActions::Action::applyFunction(FunctionPtr function_,
																	const std::vector<std::string> & argument_names_,
																	std::string result_name_)
{
	if (result_name_ == "")
	{
		result_name_ = function_->getName() + "(";
		for (size_t i = 0 ; i < argument_names_.size(); ++i)
		{
			if (i)
				result_name_ += ", ";
			result_name_ += argument_names_[i];
		}
		result_name_ += ")";
	}
	
	Action a;
	a.type = APPLY_FUNCTION;
	a.result_name = result_name_;
	a.function = function_;
	a.argument_names = argument_names_;
	return a;
}

ExpressionActions::Actions ExpressionActions::Action::getPrerequisites(Block & sample_block)
{
	Actions res;
	
	if (type == APPLY_FUNCTION)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
		ColumnsWithNameAndType arguments(argument_names.size());
		for (size_t i = 0; i < argument_names.size(); ++i)
		{
			if (!sample_block.has(argument_names[i]))
				throw Exception("Unknown identifier: '" + argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
			arguments[i] = sample_block.getByName(argument_names[i]);
		}
		
		function->getReturnTypeAndPrerequisites(arguments, result_type, res);
		
		for (size_t i = 0; i < res.size(); ++i)
		{
			if (res[i].result_name != "")
				prerequisite_names.push_back(res[i].result_name);
		}
	}
	
	return res;
}
	
void ExpressionActions::Action::prepare(Block & sample_block)
{
	if (type == APPLY_FUNCTION)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
		bool all_const = true;
		
		ColumnNumbers arguments(argument_names.size());
		for (size_t i = 0; i < argument_names.size(); ++i)
		{
			arguments[i] = sample_block.getPositionByName(argument_names[i]);
			ColumnPtr col = sample_block.getByPosition(arguments[i]).column;
			if (!col || !col->isConst())
				all_const = false;
		}
		
		ColumnNumbers prerequisites(prerequisite_names.size());
		for (size_t i = 0; i < prerequisite_names.size(); ++i)
		{
			prerequisites[i] = sample_block.getPositionByName(prerequisite_names[i]);
			ColumnPtr col = sample_block.getByPosition(prerequisites[i]).column;
			if (!col || !col->isConst())
				all_const = false;
		}
		
		ColumnPtr new_column;
		
		/// Если все аргументы и требуемые столбцы - константы, выполним функцию.
		if (all_const)
		{
			ColumnWithNameAndType new_column;
			new_column.name = result_name;
			new_column.type = result_type;
			sample_block.insert(new_column);
			
			size_t result_position = sample_block.getPositionByName(result_name);
			function->execute(sample_block, arguments, prerequisites, result_position);
			
			/// Если получилась не константа, на всякий случай будем считать результат неизвестным.
			ColumnWithNameAndType & col = sample_block.getByPosition(result_position);
			if (!col.column->isConst())
			{
				col.column = NULL;
			}
		}
		else
		{
			sample_block.insert(ColumnWithNameAndType(NULL, result_type, result_name));
		}
	}
	else if (type == ARRAY_JOIN)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		if (!sample_block.has(source_name))
			throw Exception("Unknown identifier: '" + source_name + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
		
		const DataTypeArray * array_type = dynamic_cast<const DataTypeArray *>(&*sample_block.getByName(source_name).type);
		if (!array_type)
			throw Exception("arrayJoin requires array argument", ErrorCodes::TYPE_MISMATCH);
		result_type = array_type->getNestedType();
		
		sample_block.insert(ColumnWithNameAndType(NULL, result_type, result_name));
		sample_block.erase(source_name);
	}
	else if (type == ADD_COLUMN)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
		sample_block.insert(ColumnWithNameAndType(added_column, result_type, result_name));
	}
	else
	{
		if (type == COPY_COLUMN)
			result_type = sample_block.getByName(source_name).type;
		
		execute(sample_block);
	}
}

void ExpressionActions::Action::execute(Block & block) const
{
	if (type == REMOVE_COLUMN || type == COPY_COLUMN || type == ARRAY_JOIN)
		if (!block.has(source_name))
			throw Exception("Not found column '" + source_name + "'. There are columns: " + block.dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
	
	if (type == ADD_COLUMN || type == COPY_COLUMN || type == APPLY_FUNCTION || type == ARRAY_JOIN)
		if (block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
	switch (type)
	{
		case APPLY_FUNCTION:
		{
			ColumnNumbers arguments(argument_names.size());
			for (size_t i = 0; i < argument_names.size(); ++i)
			{
				if (!block.has(argument_names[i]))
					throw Exception("Not found column: '" + argument_names[i] + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
				arguments[i] = block.getPositionByName(argument_names[i]);
			}
			
			ColumnNumbers prerequisites(prerequisite_names.size());
			for (size_t i = 0; i < prerequisite_names.size(); ++i)
			{
				if (!block.has(prerequisite_names[i]))
					throw Exception("Not found column: '" + prerequisite_names[i] + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
				prerequisites[i] = block.getPositionByName(prerequisite_names[i]);
			}
			
			ColumnWithNameAndType new_column;
			new_column.name = result_name;
			new_column.type = result_type;
			block.insert(new_column);
			
			function->execute(block, arguments, prerequisites, block.getPositionByName(result_name));
			
			break;
		}
		
		case ARRAY_JOIN:
		{
			size_t array_column = block.getPositionByName(source_name);
			
			ColumnPtr array_ptr = block.getByPosition(array_column).column;
			
			if (array_ptr->isConst())
				array_ptr = dynamic_cast<const IColumnConst &>(*array_ptr).convertToFullColumn();
			
			ColumnArray * array = dynamic_cast<ColumnArray *>(&*array_ptr);
			if (!array)
				throw Exception("arrayJoin of not array: " + array_ptr->getName(), ErrorCodes::TYPE_MISMATCH);
			
			size_t columns = block.columns();
			for (size_t i = 0; i < columns; ++i)
			{
				ColumnWithNameAndType & current = block.getByPosition(i);
				
				if (i == array_column)
				{
					ColumnWithNameAndType result;
					result.column = array->getDataPtr();
					result.type = dynamic_cast<const DataTypeArray &>(*current.type).getNestedType();
					result.name = result_name;
					
					block.erase(i);
					block.insert(i, result);
				}
				else
					current.column = current.column->replicate(array->getOffsets());
			}
			
			break;
		}
		
		case PROJECT:
		{
			Block new_block;
			
			for (size_t i = 0; i < projection.size(); ++i)
			{
				const std::string & name = projection[i].first;
				const std::string & alias = projection[i].second;
				ColumnWithNameAndType column = block.getByName(name);
				if (alias != "")
					column.name = alias;
				if (new_block.has(column.name))
					throw Exception("Column " + column.name + " already exists", ErrorCodes::DUPLICATE_COLUMN);
				new_block.insert(column);
			}
			
			block = new_block;
			
			break;
		}
		
		case REMOVE_COLUMN:
			block.erase(source_name);
			break;
			
		case ADD_COLUMN:
			block.insert(ColumnWithNameAndType(added_column->cloneResized(block.rows()), result_type, result_name));
			break;
			
		case COPY_COLUMN:
			block.insert(ColumnWithNameAndType(block.getByName(source_name).column, result_type, result_name));
			break;
			
		default:
			throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
	}
}

std::string ExpressionActions::Action::toString() const
{
	std::stringstream ss;
	switch (type)
	{
		case ADD_COLUMN:
			ss << "+" << result_name << "(" << result_type->getName() << ")" << "[" << added_column->getName() << "]";
			break;
		case REMOVE_COLUMN:
			ss << "-" << source_name;
			break;
		case COPY_COLUMN:
			ss << result_name << "(" << result_type->getName() << ")" << "=" << source_name;
			break;
		case APPLY_FUNCTION:
			ss << result_name << "(" << result_type->getName() << ")" << "= " << function->getName() << " ( ";
			for (size_t i = 0; i < argument_names.size(); ++i)
			{
				if (i)
					ss << " , ";
				ss << argument_names[i];
			}
			ss << " )";
			break;
		case ARRAY_JOIN:
			ss << result_name << "(" << result_type->getName() << ")" << "= " << "arrayJoin" << " ( " << source_name << " )";
			break;
		case PROJECT:
			ss << "{";
			for (size_t i = 0; i < projection.size(); ++i)
			{
				if (i)
					ss << ", ";
				ss << projection[i].first;
				if (projection[i].second != "" && projection[i].second != projection[i].first)
					ss << "=>" << projection[i].second;
			}
			ss << "}";
			break;
		default:
			throw Exception("Unexpected Action type", ErrorCodes::LOGICAL_ERROR);
	}
	
	return ss.str();
}

void ExpressionActions::checkLimits(Block & block) const
{
	const Limits & limits = settings.limits;
	if (limits.max_temporary_columns && block.columns() > limits.max_temporary_columns)
		throw Exception("Too many temporary columns: " + block.dumpNames()
		+ ". Maximum: " + Poco::NumberFormatter::format(limits.max_temporary_columns),
						ErrorCodes::TOO_MUCH_TEMPORARY_COLUMNS);
	
	size_t non_const_columns = 0;
	for (size_t i = 0, size = block.columns(); i < size; ++i)
		if (block.getByPosition(i).column && !block.getByPosition(i).column->isConst())
			++non_const_columns;
		
	if (limits.max_temporary_non_const_columns && non_const_columns > limits.max_temporary_non_const_columns)
	{
		std::stringstream list_of_non_const_columns;
		for (size_t i = 0, size = block.columns(); i < size; ++i)
			if (!block.getByPosition(i).column->isConst())
				list_of_non_const_columns << (i == 0 ? "" : ", ") << block.getByPosition(i).name;
			
			throw Exception("Too many temporary non-const columns: " + list_of_non_const_columns.str()
			+ ". Maximum: " + Poco::NumberFormatter::format(limits.max_temporary_non_const_columns),
							ErrorCodes::TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS);
	}
}

void ExpressionActions::add(const Action & action)
{
	NameSet temp_names;
	addImpl(action, temp_names);
	
	checkLimits(sample_block);
}

void ExpressionActions::addImpl(Action action, NameSet & current_names)
{
	if (sample_block.has(action.result_name))
		return;
	
	if (current_names.count(action.result_name))
		throw Exception("Cyclic function prerequisites: " + action.result_name, ErrorCodes::LOGICAL_ERROR);
	
	current_names.insert(action.result_name);
	
	Actions prerequisites = action.getPrerequisites(sample_block);
	
	for (size_t i = 0; i < prerequisites.size(); ++i)
		addImpl(prerequisites[i], current_names);
	
	action.prepare(sample_block);
	actions.push_back(action);
	
	current_names.erase(action.result_name);
}

void ExpressionActions::prependProjectInput()
{
	actions.insert(actions.begin(), Action::project(getRequiredColumns()));
}

void ExpressionActions::execute(Block & block) const
{
	for (size_t i = 0; i < actions.size(); ++i)
	{
		actions[i].execute(block);
		
		checkLimits(block);
	}
}

static std::string getAnyColumn(const NamesAndTypesList & columns)
{
	NamesAndTypesList::const_iterator it = columns.begin();
	
	size_t min_size = it->second->isNumeric() ? it->second->getSizeOfField() : 100;
	String res = it->first;
	for (; it != columns.end(); ++it)
	{
		size_t current_size = it->second->isNumeric() ? it->second->getSizeOfField() : 100;
		if (current_size < min_size)
		{
			min_size = current_size;
			res = it->first;
		}
	}
	
	return res;
}

void ExpressionActions::finalize(const Names & output_columns)
{
	typedef std::set<std::string> NameSet;
	
	NameSet final_columns;
	for (size_t i = 0; i < output_columns.size(); ++i)
	{
		const std::string name = output_columns[i];
		if (!sample_block.has(name))
			throw Exception("Unknown column: " + name + ", there are only columns "
							+ sample_block.dumpNames(), ErrorCodes::UNKNOWN_IDENTIFIER);
		final_columns.insert(name);
	}
	
	/// Не будем оставлять блок пустым, чтобы не потерять количество строк в нем.
	if (final_columns.empty())
		final_columns.insert(getAnyColumn(input_columns));
	
	NameSet used_columns = final_columns;
	
	for (size_t i = 0; i < actions.size(); ++i)
	{
		Action & action = actions[i];
		
		used_columns.insert(action.source_name);
		used_columns.insert(action.argument_names.begin(), action.argument_names.end());
		used_columns.insert(action.prerequisite_names.begin(), action.prerequisite_names.end());
		
		for (size_t j = 0; j < actions[i].projection.size(); ++j)
			used_columns.insert(actions[i].projection[j].first);
	}
	for (NamesAndTypesList::iterator it = input_columns.begin(); it != input_columns.end();)
	{
		NamesAndTypesList::iterator it0 = it;
		++it;
		if (!used_columns.count(it0->first))
		{
			if (sample_block.has(it0->first))
				sample_block.erase(it0->first);
			input_columns.erase(it0);
		}
	}
	
	for (int i = static_cast<int>(sample_block.columns()) - 1; i >= 0; --i)
	{
		const std::string & name = sample_block.getByPosition(i).name;
		if (!final_columns.count(name))
			add(Action::removeColumn(name));
	}
}

std::string ExpressionActions::getID() const
{
	std::stringstream ss;
	
	for (size_t i = 0; i < actions.size(); ++i)
	{
		if (i)
			ss << ", ";
		if (actions[i].type == Action::APPLY_FUNCTION || actions[i].type == Action::ARRAY_JOIN)
			ss << actions[i].result_name;
	}
	
	ss << ": {";
	NamesAndTypesList output_columns = sample_block.getColumnsList();
	for (NamesAndTypesList::const_iterator it = output_columns.begin(); it != output_columns.end(); ++it)
	{
		if (it != output_columns.begin())
			ss << ", ";
		ss << it->first;
	}
	ss << "}";
	
	return ss.str();
}

std::string ExpressionActions::dumpActions() const
{
	std::stringstream ss;
	
	ss << "input:\n";
	for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
		ss << it->first << " " << it->second->getName() << "\n";
	
	ss << "\nactions:\n";
	for (size_t i = 0; i < actions.size(); ++i)
		ss << actions[i].toString() << '\n';
	
	ss << "\noutput:\n";
	NamesAndTypesList output_columns = sample_block.getColumnsList();
	for (NamesAndTypesList::const_iterator it = output_columns.begin(); it != output_columns.end(); ++it)
		ss << it->first << " " << it->second->getName() << "\n";
	
	return ss.str();
}

}
