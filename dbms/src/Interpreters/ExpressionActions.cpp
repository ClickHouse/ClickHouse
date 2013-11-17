#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Functions/IFunction.h>
#include <set>

namespace DB
{
	
Names ExpressionActions::Action::getNeededColumns() const
{
	Names res = argument_names;
	
	res.insert(res.end(), prerequisite_names.begin(), prerequisite_names.end());
	
	res.insert(res.end(), array_joined_columns.begin(), array_joined_columns.end());
	
	for (size_t i = 0; i < projection.size(); ++i)
	{
		res.push_back(projection[i].first);
	}
	
	if (!source_name.empty())
		res.push_back(source_name);
	
	return res;
}

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
	if (!source_name.empty())
		source_position = sample_block.getPositionByName(source_name);

	if (type == APPLY_FUNCTION)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
		bool all_const = true;
		
		argument_positions.resize(argument_names.size());
		for (size_t i = 0; i < argument_names.size(); ++i)
		{
			argument_positions[i] = sample_block.getPositionByName(argument_names[i]);
			ColumnPtr col = sample_block.getByPosition(argument_positions[i]).column;
			if (!col || !col->isConst())
				all_const = false;
		}
		
		prerequisite_positions.resize(prerequisite_names.size());
		for (size_t i = 0; i < prerequisite_names.size(); ++i)
		{
			prerequisite_positions[i] = sample_block.getPositionByName(prerequisite_names[i]);
			ColumnPtr col = sample_block.getByPosition(prerequisite_positions[i]).column;
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
			function->execute(sample_block, argument_positions, prerequisite_positions, result_position);
			
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
			result_position = sample_block.columns() - 1;
		}
	}
	else if (type == ARRAY_JOIN)
	{
		array_joined_columns_positions.resize(array_joined_columns.size());

		size_t i = 0;
		for (NameSet::iterator it = array_joined_columns.begin(); it != array_joined_columns.end(); ++it, ++i)
		{
			array_joined_columns_positions[i] = sample_block.getPositionByName(*it);
			ColumnWithNameAndType & current = sample_block.getByPosition(array_joined_columns_positions[i]);
			const DataTypeArray * array_type = dynamic_cast<const DataTypeArray *>(&*current.type);
			if (!array_type)
				throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);
			current.type = array_type->getNestedType();
		}
	}
	else if (type == ADD_COLUMN)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
		
		sample_block.insert(ColumnWithNameAndType(added_column, result_type, result_name));
		result_position = sample_block.columns() - 1;
	}
	else if (type == PROJECT)
	{
		projection_positions.resize(projection.size());
		for (size_t i = 0; i < projection.size(); ++i)
			projection_positions[i] = std::make_pair(sample_block.getPositionByName(projection[i].first), projection[i].second);

		execute(sample_block);
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
	switch (type)
	{
		case APPLY_FUNCTION:
		{
			ColumnWithNameAndType new_column;
			new_column.name = result_name;
			new_column.type = result_type;
			block.insert(new_column);
			
			function->execute(block, argument_positions, prerequisite_positions, result_position);
			
			break;
		}
		
		case ARRAY_JOIN:
		{
			if (array_joined_columns.empty())
				throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);
			ColumnPtr any_array_ptr = block.getByPosition(array_joined_columns_positions[0]).column;
			if (any_array_ptr->isConst())
				any_array_ptr = dynamic_cast<const IColumnConst &>(*any_array_ptr).convertToFullColumn();
			const ColumnArray * any_array = dynamic_cast<const ColumnArray *>(&*any_array_ptr);
			if (!any_array)
				throw Exception("ARRAY JOIN of not array: " + array_joined_columns_positions[0], ErrorCodes::TYPE_MISMATCH);

			size_t columns = block.columns();
			for (size_t i = 0; i < columns; ++i)
			{
				ColumnWithNameAndType & current = block.getByPosition(i);
				
				if (array_joined_columns.count(current.name))
				{
					if (!dynamic_cast<const DataTypeArray *>(&*current.type))
						throw Exception("ARRAY JOIN of not array: " + current.name, ErrorCodes::TYPE_MISMATCH);
					
					ColumnPtr array_ptr = current.column;
					if (array_ptr->isConst())
						array_ptr = dynamic_cast<const IColumnConst &>(*array_ptr).convertToFullColumn();
					
					const ColumnArray & array = dynamic_cast<const ColumnArray &>(*array_ptr);
					if (!array.hasEqualOffsets(dynamic_cast<const ColumnArray &>(*any_array_ptr)))
						throw Exception("Sizes of ARRAY-JOIN-ed arrays do not match", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

					current.column = dynamic_cast<const ColumnArray &>(*array_ptr).getDataPtr();
					current.type = dynamic_cast<const DataTypeArray &>(*current.type).getNestedType();
				}
				else
				{
					current.column = current.column->replicate(any_array->getOffsets());
				}
			}

			break;
		}
		
		case PROJECT:
		{
			Block new_block;
			
			for (size_t i = 0; i < projection_positions.size(); ++i)
			{
				ColumnWithNameAndType column = block.getByPosition(projection_positions[i].first);
				const std::string & alias = projection_positions[i].second;
				if (alias != "")
					column.name = alias;
				new_block.insert(column);
			}
			
			block.swap(new_block);
			
			break;
		}
		
		case REMOVE_COLUMN:
			block.erase(source_name);
			break;
			
		case ADD_COLUMN:
			block.insert(ColumnWithNameAndType(added_column->cloneResized(block.rows()), result_type, result_name));
			break;
			
		case COPY_COLUMN:
			block.insert(ColumnWithNameAndType(block.getByPosition(source_position).column, result_type, result_name));
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
			ss << "ARRAY JOIN ";
			for (NameSet::iterator it = array_joined_columns.begin(); it != array_joined_columns.end(); ++it)
			{
				if (it != array_joined_columns.begin())
					ss << ", ";
				ss << *it;
			}
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
		+ ". Maximum: " + toString(limits.max_temporary_columns),
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
			+ ". Maximum: " + toString(limits.max_temporary_non_const_columns),
							ErrorCodes::TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS);
	}
}

void ExpressionActions::addInput(const ColumnWithNameAndType & column)
{
	input_columns.push_back(NameAndTypePair(column.name, column.type));
	sample_block.insert(column);
}

void ExpressionActions::addInput(const NameAndTypePair & column)
{
	addInput(ColumnWithNameAndType(NULL, column.second, column.first));
}

void ExpressionActions::add(const Action & action, Names & out_new_columns)
{
	NameSet temp_names;
	addImpl(action, temp_names, out_new_columns);
}

void ExpressionActions::add(const Action & action)
{
	NameSet temp_names;
	Names new_names;
	addImpl(action, temp_names, new_names);
}

void ExpressionActions::addImpl(Action action, NameSet & current_names, Names & new_names)
{
	if (sample_block.has(action.result_name))
		return;
	
	if (current_names.count(action.result_name))
		throw Exception("Cyclic function prerequisites: " + action.result_name, ErrorCodes::LOGICAL_ERROR);
	
	current_names.insert(action.result_name);
	
	if (action.result_name != "")
		new_names.push_back(action.result_name);
	new_names.insert(new_names.end(), action.array_joined_columns.begin(), action.array_joined_columns.end());
	
	Actions prerequisites = action.getPrerequisites(sample_block);
	
	for (size_t i = 0; i < prerequisites.size(); ++i)
		addImpl(prerequisites[i], current_names, new_names);
	
	action.prepare(sample_block);
	actions.push_back(action);
	
	current_names.erase(action.result_name);
}

void ExpressionActions::prependProjectInput()
{
	Action action = Action::project(getRequiredColumns());
	action.prepare(sample_block);
	actions.insert(actions.begin(), action);
}

void ExpressionActions::execute(Block & block) const
{
	for (size_t i = 0; i < actions.size(); ++i)
	{
		actions[i].execute(block);
		
		checkLimits(block);
	}
}

std::string ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
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
		final_columns.insert(getSmallestColumn(input_columns));
	
	/// Какие столбцы нужны, чтобы выполнить действия от текущего до последнего.
	NameSet needed_columns = final_columns;
	/// Какие столбцы никто не будет трогать от текущего действия до последнего.
	NameSet unmodified_columns;
	
	{
		NamesAndTypesList sample_columns = sample_block.getColumnsList();
		for (NamesAndTypesList::iterator it = sample_columns.begin(); it != sample_columns.end(); ++it)
			unmodified_columns.insert(it->first);
	}
	
	/// Будем идти с конца и поодерживать множество нужных на данном этапе столбцов.
	/// Будем выбрасывать ненужные действия, хотя обычно их нет по построению.
	for (int i = static_cast<int>(actions.size()) - 1; i >= 0; --i)
	{
		Action & action = actions[i];
		Names in = action.getNeededColumns();

		if (action.type == Action::PROJECT)
		{
			needed_columns = NameSet(in.begin(), in.end());
			unmodified_columns.clear();
		}
		else if (action.type == Action::ARRAY_JOIN)
		{
			/// Не будем ARRAY JOIN-ить столбцы, которые дальше не используются.
			/// Обычно такие столбцы не используются и до ARRAY JOIN, и поэтому выбрасываются дальше в этой функции.
			/// Не будем убирать все столбцы, чтобы не потерять количество строк.
			NameSet::iterator it = action.array_joined_columns.begin();
			while (it != action.array_joined_columns.end() && action.array_joined_columns.size() > 1)
			{
				NameSet::iterator jt = it;
				++it;
				if (!needed_columns.count(*jt))
				{
					action.array_joined_columns.erase(jt);
				}
			}
		}
		else
		{
			std::string out = action.result_name;
			if (!out.empty())
			{
				/// Если результат не используется и нет побочных эффектов, выбросим действие.
				if (!needed_columns.count(out) &&
					(action.type == Action::APPLY_FUNCTION
					|| action.type == Action::ADD_COLUMN
					|| action.type == Action::COPY_COLUMN))
				{
					actions.erase(actions.begin() + i);
					
					if (unmodified_columns.count(out))
					{
						sample_block.erase(out);
						unmodified_columns.erase(out);
					}
					
					continue;
				}
				
				unmodified_columns.erase(out);
				
				needed_columns.erase(out);
			}
			
			needed_columns.insert(in.begin(), in.end());
		}
	}
	
	/// Не будем выбрасывать все входные столбцы, чтобы не потерять количество строк в блоке.
	if (needed_columns.empty() && !input_columns.empty())
		needed_columns.insert(getSmallestColumn(input_columns));
	
	for (NamesAndTypesList::iterator it = input_columns.begin(); it != input_columns.end();)
	{
		NamesAndTypesList::iterator it0 = it;
		++it;
		if (!needed_columns.count(it0->first))
		{
			if (unmodified_columns.count(it0->first))
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
	
	optimize();
	
	checkLimits(sample_block);
}

std::string ExpressionActions::getID() const
{
	std::stringstream ss;
	
	for (size_t i = 0; i < actions.size(); ++i)
	{
		if (i)
			ss << ", ";
		if (actions[i].type == Action::APPLY_FUNCTION)
			ss << actions[i].result_name;
		if (actions[i].type == Action::ARRAY_JOIN)
		{
			ss << "{";
			for (NameSet::const_iterator it = actions[i].array_joined_columns.begin();
				 it != actions[i].array_joined_columns.end(); ++it)
			{
				if (it != actions[i].array_joined_columns.begin())
					ss << ", ";
				ss << *it;
			}
			ss << "}";
		}
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

void ExpressionActions::optimize()
{
	optimizeArrayJoin();
}

void ExpressionActions::optimizeArrayJoin()
{
	const size_t NONE = actions.size();
	size_t first_array_join = NONE;
	
	/// Столбцы, для вычисления которых нужен arrayJoin.
	/// Действия для их добавления нельзя переместить левее arrayJoin.
	NameSet array_joined_columns;
	
	/// Столбцы, нужные для вычисления arrayJoin или тех, кто от него зависит.
	/// Действия для их удаления нельзя переместить левее arrayJoin.
	NameSet array_join_dependencies;
	
	for (size_t i = 0; i < actions.size(); ++i)
	{
		/// Не будем перемещать действия правее проецирования (тем более, что их там обычно нет).
		if (actions[i].type == Action::PROJECT)
			break;
		
		bool depends_on_array_join = false;
		Names needed;
		
		if (actions[i].type == Action::ARRAY_JOIN)
		{
			depends_on_array_join = true;
			needed = actions[i].getNeededColumns();
		}
		else
		{
			if (first_array_join == NONE)
				continue;
			
			needed = actions[i].getNeededColumns();
			
			for (size_t j = 0; j < needed.size(); ++j)
			{
				if (array_joined_columns.count(needed[j]))
				{
					depends_on_array_join = true;
					break;
				}
			}
		}
		
		if (depends_on_array_join)
		{
			if (first_array_join == NONE)
				first_array_join = i;
			
			if (actions[i].result_name != "")
				array_joined_columns.insert(actions[i].result_name);
			array_joined_columns.insert(actions[i].array_joined_columns.begin(), actions[i].array_joined_columns.end());
			
			array_join_dependencies.insert(needed.begin(), needed.end());
		}
		else
		{
			bool can_move = false;
			
			if (actions[i].type == Action::REMOVE_COLUMN)
			{
				/// Если удаляем столбец, не нужный для arrayJoin (и тех, кто от него зависит), можно его удалить до arrayJoin.
				can_move = !array_join_dependencies.count(actions[i].source_name);
			}
			else
			{
				/// Если действие не удаляет столбцы и не зависит от результата arrayJoin, можно сделать его до arrayJoin.
				can_move = true;
			}
			
			/// Переместим текущее действие в позицию сразу перед первым arrayJoin.
			if (can_move)
			{
				/// Переместим i-й элемент в позицию first_array_join.
				std::rotate(actions.begin() + first_array_join, actions.begin() + i, actions.begin() + i + 1);
				++first_array_join;
			}
		}
	}
}

}
