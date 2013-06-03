#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <set>

namespace DB
{

void ExpressionActions::Action::prepare(Block & sample_block)
{
	if (type == APPLY_FUNCTION)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
		DataTypes types(argument_names.size());
		for (size_t i = 0; i < argument_names.size(); ++i)
		{
			if (!sample_block.has(argument_names[i]))
				throw Exception("Unknown identifier: '" + argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
			types[i] = sample_block.getByName(argument_names[i]).type;
		}
		
		if (FunctionTupleElement * func_tuple_elem = dynamic_cast<FunctionTupleElement *>(&*function))
		{
			/// Особый случай - для функции tupleElement обычный метод getReturnType не работает.
			if (argument_names.size() != 2)
				throw Exception("Function tupleElement requires exactly two arguments: tuple and element index.",
								ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			const ColumnConstUInt8 * index_col = dynamic_cast<const ColumnConstUInt8 *>(&*sample_block.getByName(argument_names[1]).column);
			result_type = func_tuple_elem->getReturnType(types, index_col->getData());
		}
		else
		{
			result_type = function->getReturnType(types);
		}
		
		sample_block.insert(ColumnWithNameAndType(NULL, result_type, result_name));
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

void ExpressionActions::Action::execute(Block & block)
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
			
			ColumnWithNameAndType new_column;
			new_column.name = result_name;
			new_column.type = result_type;
			block.insert(new_column);
			
			function->execute(block, arguments, block.getPositionByName(result_name));
			
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

void ExpressionActions::checkLimits(Block & block)
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
	if (sample_block.has(action.result_name))
		return;
	actions.push_back(action);
	actions.back().prepare(sample_block);
	
	checkLimits(sample_block);
}

void ExpressionActions::prependProjectInput()
{
	actions.insert(actions.begin(), Action::project(getRequiredColumns()));
}

void ExpressionActions::execute(Block & block)
{
	for (size_t i = 0; i < actions.size(); ++i)
	{
		actions[i].execute(block);
		
		checkLimits(block);
	}
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
	
	NameSet used_columns = final_columns;
	
	for (size_t i = 0; i < actions.size(); ++i)
	{
		used_columns.insert(actions[i].source_name);
		for (size_t j = 0; j < actions[i].argument_names.size(); ++j)
		{
			used_columns.insert(actions[i].argument_names[j]);
		}
		for (size_t j = 0; j < actions[i].projection.size(); ++j)
		{
			used_columns.insert(actions[i].projection[j].first);
		}
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
		/// Не удаляем последний столбец.
		if (!final_columns.count(name) && sample_block.columns() > 1)
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
