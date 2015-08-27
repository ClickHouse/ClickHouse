#include <DB/Common/ProfileEvents.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Interpreters/Join.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/FunctionsArray.h>
#include <set>


namespace DB
{

Names ExpressionAction::getNeededColumns() const
{
	Names res = argument_names;

	res.insert(res.end(), prerequisite_names.begin(), prerequisite_names.end());
	res.insert(res.end(), array_joined_columns.begin(), array_joined_columns.end());

	for (const auto & column : projection)
		res.push_back(column.first);

	if (!source_name.empty())
		res.push_back(source_name);

	return res;
}

ExpressionAction ExpressionAction::applyFunction(FunctionPtr function_,
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

	ExpressionAction a;
	a.type = APPLY_FUNCTION;
	a.result_name = result_name_;
	a.function = function_;
	a.argument_names = argument_names_;
	return a;
}

ExpressionActions::Actions ExpressionAction::getPrerequisites(Block & sample_block)
{
	ExpressionActions::Actions res;

	if (type == APPLY_FUNCTION)
	{
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

		ColumnsWithTypeAndName arguments(argument_names.size());
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

void ExpressionAction::prepare(Block & sample_block)
{
//	std::cerr << "preparing: " << toString() << std::endl;

	/** Константные выражения следует вычислить, и положить результат в sample_block.
	  * Для неконстантных столбцов, следует в качестве column в sample_block положить nullptr.
	  *
	  * Тот факт, что только для константных выражений column != nullptr,
	  *  может использоваться в дальнейшем при оптимизации запроса.
	  */

	switch (type)
	{
		case APPLY_FUNCTION:
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
				size_t result_position = sample_block.columns();

				ColumnWithTypeAndName new_column;
				new_column.name = result_name;
				new_column.type = result_type;
				sample_block.insert(new_column);

				function->execute(sample_block, arguments, prerequisites, result_position);

				/// Если получилась не константа, на всякий случай будем считать результат неизвестным.
				ColumnWithTypeAndName & col = sample_block.getByPosition(result_position);
				if (!col.column->isConst())
					col.column = nullptr;
			}
			else
			{
				sample_block.insert(ColumnWithTypeAndName(nullptr, result_type, result_name));
			}

			break;
		}

		case ARRAY_JOIN:
		{
			for (NameSet::iterator it = array_joined_columns.begin(); it != array_joined_columns.end(); ++it)
			{
				ColumnWithTypeAndName & current = sample_block.getByName(*it);
				const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*current.type);
				if (!array_type)
					throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);
				current.type = array_type->getNestedType();
				current.column = nullptr;
			}

			break;
		}

		case JOIN:
		{
			for (const auto & col : columns_added_by_join)
				sample_block.insert(ColumnWithTypeAndName(nullptr, col.type, col.name));

			break;
		}

		case PROJECT:
		{
			Block new_block;

			for (size_t i = 0; i < projection.size(); ++i)
			{
				const std::string & name = projection[i].first;
				const std::string & alias = projection[i].second;
				ColumnWithTypeAndName column = sample_block.getByName(name);
				if (alias != "")
					column.name = alias;
				new_block.insert(column);
			}

			sample_block.swap(new_block);
			break;
		}

		case REMOVE_COLUMN:
		{
			sample_block.erase(source_name);
			break;
		}

		case ADD_COLUMN:
		{
			if (sample_block.has(result_name))
				throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

			sample_block.insert(ColumnWithTypeAndName(added_column, result_type, result_name));
			break;
		}

		case COPY_COLUMN:
		{
			result_type = sample_block.getByName(source_name).type;
			sample_block.insert(ColumnWithTypeAndName(sample_block.getByName(source_name).column, result_type, result_name));
			break;
		}

		default:
			throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
	}
}

void ExpressionAction::execute(Block & block) const
{
//	std::cerr << "executing: " << toString() << std::endl;

	if (type == REMOVE_COLUMN || type == COPY_COLUMN)
		if (!block.has(source_name))
			throw Exception("Not found column '" + source_name + "'. There are columns: " + block.dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

	if (type == ADD_COLUMN || type == COPY_COLUMN || type == APPLY_FUNCTION)
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

			ColumnWithTypeAndName new_column;
			new_column.name = result_name;
			new_column.type = result_type;
			block.insert(new_column);

			ProfileEvents::increment(ProfileEvents::FunctionExecute);
			function->execute(block, arguments, prerequisites, block.getPositionByName(result_name));

			break;
		}

		case ARRAY_JOIN:
		{
			if (array_joined_columns.empty())
				throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);
			ColumnPtr any_array_ptr = block.getByName(*array_joined_columns.begin()).column;
			if (any_array_ptr->isConst())
				any_array_ptr = dynamic_cast<const IColumnConst &>(*any_array_ptr).convertToFullColumn();
			const ColumnArray * any_array = typeid_cast<const ColumnArray *>(&*any_array_ptr);
			if (!any_array)
				throw Exception("ARRAY JOIN of not array: " + *array_joined_columns.begin(), ErrorCodes::TYPE_MISMATCH);

			/// Если LEFT ARRAY JOIN, то создаём столбцы, в которых пустые массивы заменены на массивы с одним элементом - значением по-умолчанию.
			std::map<String, ColumnPtr> non_empty_array_columns;
			if (array_join_is_left)
			{
				for (const auto & name : array_joined_columns)
				{
					auto src_col = block.getByName(name);

					Block tmp_block{src_col, {{}, src_col.type, {}}};

					FunctionEmptyArrayToSingle().execute(tmp_block, {0}, 1);
					non_empty_array_columns[name] = tmp_block.getByPosition(1).column;
				}

				any_array_ptr = non_empty_array_columns.begin()->second;
				any_array = typeid_cast<const ColumnArray *>(&*any_array_ptr);
			}

			size_t columns = block.columns();
			for (size_t i = 0; i < columns; ++i)
			{
				ColumnWithTypeAndName & current = block.getByPosition(i);

				if (array_joined_columns.count(current.name))
				{
					if (!typeid_cast<const DataTypeArray *>(&*current.type))
						throw Exception("ARRAY JOIN of not array: " + current.name, ErrorCodes::TYPE_MISMATCH);

					ColumnPtr array_ptr = array_join_is_left ? non_empty_array_columns[current.name] : current.column;

					if (array_ptr->isConst())
						array_ptr = dynamic_cast<const IColumnConst &>(*array_ptr).convertToFullColumn();

					const ColumnArray & array = typeid_cast<const ColumnArray &>(*array_ptr);
					if (!array.hasEqualOffsets(typeid_cast<const ColumnArray &>(*any_array_ptr)))
						throw Exception("Sizes of ARRAY-JOIN-ed arrays do not match", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

					current.column = typeid_cast<const ColumnArray &>(*array_ptr).getDataPtr();
					current.type = typeid_cast<const DataTypeArray &>(*current.type).getNestedType();
				}
				else
				{
					current.column = current.column->replicate(any_array->getOffsets());
				}
			}

			break;
		}

		case JOIN:
		{
			join->joinBlock(block);
			break;
		}

		case PROJECT:
		{
			Block new_block;

			for (size_t i = 0; i < projection.size(); ++i)
			{
				const std::string & name = projection[i].first;
				const std::string & alias = projection[i].second;
				ColumnWithTypeAndName column = block.getByName(name);
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
			block.insert(ColumnWithTypeAndName(added_column->cloneResized(block.rowsInFirstColumn()), result_type, result_name));
			break;

		case COPY_COLUMN:
			block.insert(ColumnWithTypeAndName(block.getByName(source_name).column, result_type, result_name));
			break;

		default:
			throw Exception("Unknown action type", ErrorCodes::UNKNOWN_ACTION);
	}
}


void ExpressionAction::executeOnTotals(Block & block) const
{
	if (type != JOIN)
		execute(block);
	else
		join->joinTotals(block);
}


std::string ExpressionAction::toString() const
{
	std::stringstream ss;
	switch (type)
	{
		case ADD_COLUMN:
			ss << "ADD " << result_name << " " << result_type->getName() << " " << added_column->getName();
			break;

		case REMOVE_COLUMN:
			ss << "REMOVE " << source_name;
			break;

		case COPY_COLUMN:
			ss << "COPY " << result_name << " = " << source_name;
			break;

		case APPLY_FUNCTION:
			ss << "FUNCTION " << result_name << " " << result_type->getName() << " = " << function->getName() << "(";
			for (size_t i = 0; i < argument_names.size(); ++i)
			{
				if (i)
					ss << ", ";
				ss << argument_names[i];
			}
			ss << ")";
			break;

		case ARRAY_JOIN:
			ss << (array_join_is_left ? "LEFT " : "") << "ARRAY JOIN ";
			for (NameSet::const_iterator it = array_joined_columns.begin(); it != array_joined_columns.end(); ++it)
			{
				if (it != array_joined_columns.begin())
					ss << ", ";
				ss << *it;
			}
			break;

		case JOIN:
			ss << "JOIN ";
			for (NamesAndTypesList::const_iterator it = columns_added_by_join.begin(); it != columns_added_by_join.end(); ++it)
			{
				if (it != columns_added_by_join.begin())
					ss << ", ";
				ss << it->name;
			}
			break;

		case PROJECT:
			ss << "PROJECT ";
			for (size_t i = 0; i < projection.size(); ++i)
			{
				if (i)
					ss << ", ";
				ss << projection[i].first;
				if (projection[i].second != "" && projection[i].second != projection[i].first)
					ss << " AS " << projection[i].second;
			}
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

	if (limits.max_temporary_non_const_columns)
	{
		size_t non_const_columns = 0;
		for (size_t i = 0, size = block.columns(); i < size; ++i)
			if (block.getByPosition(i).column && !block.getByPosition(i).column->isConst())
				++non_const_columns;

		if (non_const_columns > limits.max_temporary_non_const_columns)
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
}

void ExpressionActions::addInput(const ColumnWithTypeAndName & column)
{
	input_columns.emplace_back(column.name, column.type);
	sample_block.insert(column);
}

void ExpressionActions::addInput(const NameAndTypePair & column)
{
	addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));
}

void ExpressionActions::add(const ExpressionAction & action, Names & out_new_columns)
{
	NameSet temp_names;
	addImpl(action, temp_names, out_new_columns);
}

void ExpressionActions::add(const ExpressionAction & action)
{
	NameSet temp_names;
	Names new_names;
	addImpl(action, temp_names, new_names);
}

void ExpressionActions::addImpl(ExpressionAction action, NameSet & current_names, Names & new_names)
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
	actions.insert(actions.begin(), ExpressionAction::project(getRequiredColumns()));
}

void ExpressionActions::prependArrayJoin(const ExpressionAction & action, const Block & sample_block)
{
	if (action.type != ExpressionAction::ARRAY_JOIN)
		throw Exception("ARRAY_JOIN action expected", ErrorCodes::LOGICAL_ERROR);

	NameSet array_join_set(action.array_joined_columns.begin(), action.array_joined_columns.end());
	for (auto & it : input_columns)
	{
		if (array_join_set.count(it.name))
		{
			array_join_set.erase(it.name);
			it.type = new DataTypeArray(it.type);
		}
	}
	for (const std::string & name : array_join_set)
	{
		input_columns.emplace_back(name, sample_block.getByName(name).type);
		actions.insert(actions.begin(), ExpressionAction::removeColumn(name));
	}

	actions.insert(actions.begin(), action);
	optimizeArrayJoin();
}


bool ExpressionActions::popUnusedArrayJoin(const Names & required_columns, ExpressionAction & out_action)
{
	if (actions.empty() || actions.back().type != ExpressionAction::ARRAY_JOIN)
		return false;
	NameSet required_set(required_columns.begin(), required_columns.end());
	for (const std::string & name : actions.back().array_joined_columns)
	{
		if (required_set.count(name))
			return false;
	}
	for (const std::string & name : actions.back().array_joined_columns)
	{
		DataTypePtr & type = sample_block.getByName(name).type;
		type = new DataTypeArray(type);
	}
	out_action = actions.back();
	actions.pop_back();
	return true;
}

void ExpressionActions::execute(Block & block) const
{
	for (const auto & action : actions)
	{
		action.execute(block);
		checkLimits(block);
	}
}

void ExpressionActions::executeOnTotals(Block & block) const
{
	/// Если в подзапросе для JOIN-а есть totals, а у нас нет, то возьмём блок со значениями по-умолчанию вместо totals.
	if (!block)
	{
		bool has_totals_in_join = false;
		for (const auto & action : actions)
		{
			if (action.join && action.join->hasTotals())
			{
				has_totals_in_join = true;
				break;
			}
		}

		if (has_totals_in_join)
		{
			for (const auto & name_and_type : input_columns)
			{
				ColumnWithTypeAndName elem(name_and_type.type->createColumn(), name_and_type.type, name_and_type.name);
				elem.column->insertDefault();
				block.insert(elem);
			}
		}
		else
			return;	/// Нечего JOIN-ить.
	}

	for (const auto & action : actions)
		action.executeOnTotals(block);
}

std::string ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
{
	NamesAndTypesList::const_iterator it = columns.begin();
	if (it == columns.end())
		throw Exception("No available columns", ErrorCodes::LOGICAL_ERROR);

	size_t min_size = it->type->isNumeric() ? it->type->getSizeOfField() : 100;
	String res = it->name;
	for (; it != columns.end(); ++it)
	{
		size_t current_size = it->type->isNumeric() ? it->type->getSizeOfField() : 100;
		if (current_size < min_size)
		{
			min_size = current_size;
			res = it->name;
		}
	}

	return res;
}

void ExpressionActions::finalize(const Names & output_columns)
{
//	std::cerr << "finalize\n";

	NameSet final_columns;
	for (size_t i = 0; i < output_columns.size(); ++i)
	{
		const std::string name = output_columns[i];
		if (!sample_block.has(name))
			throw Exception("Unknown column: " + name + ", there are only columns "
							+ sample_block.dumpNames(), ErrorCodes::UNKNOWN_IDENTIFIER);
		final_columns.insert(name);
	}

	/// Какие столбцы нужны, чтобы выполнить действия от текущего до последнего.
	NameSet needed_columns = final_columns;
	/// Какие столбцы никто не будет трогать от текущего действия до последнего.
	NameSet unmodified_columns;

	{
		NamesAndTypesList sample_columns = sample_block.getColumnsList();
		for (NamesAndTypesList::iterator it = sample_columns.begin(); it != sample_columns.end(); ++it)
			unmodified_columns.insert(it->name);
	}

	/// Будем идти с конца и поддерживать множество нужных на данном этапе столбцов.
	/// Будем выбрасывать ненужные действия, хотя обычно их нет по построению.
	for (int i = static_cast<int>(actions.size()) - 1; i >= 0; --i)
	{
		ExpressionAction & action = actions[i];
		Names in = action.getNeededColumns();

		if (action.type == ExpressionAction::PROJECT)
		{
			needed_columns = NameSet(in.begin(), in.end());
			unmodified_columns.clear();
		}
		else if (action.type == ExpressionAction::ARRAY_JOIN)
		{
			/// Не будем ARRAY JOIN-ить столбцы, которые дальше не используются.
			/// Обычно такие столбцы не используются и до ARRAY JOIN, и поэтому выбрасываются дальше в этой функции.
			/// Не будем убирать все столбцы, чтобы не потерять количество строк.
			for (auto it = action.array_joined_columns.begin(); it != action.array_joined_columns.end();)
			{
				bool need = needed_columns.count(*it);
				if (!need && action.array_joined_columns.size() > 1)
				{
					action.array_joined_columns.erase(it++);
				}
				else
				{
					needed_columns.insert(*it);
					unmodified_columns.erase(*it);

					/// Если никакие результаты ARRAY JOIN не используются, принудительно оставим на выходе произвольный столбец,
					///  чтобы не потерять количество строк.
					if (!need)
						final_columns.insert(*it);

					++it;
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
					(action.type == ExpressionAction::APPLY_FUNCTION
					|| action.type == ExpressionAction::ADD_COLUMN
					|| action.type == ExpressionAction::COPY_COLUMN))
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

				/** Если функция - константное выражение, то заменим действие на добавление столбца-константы - результата.
				  * То есть, осуществляем constant folding.
				  */
				if (action.type == ExpressionAction::APPLY_FUNCTION && sample_block.has(out))
				{
					auto & result = sample_block.getByName(out);
					if (!result.column.isNull())
					{
						action.type = ExpressionAction::ADD_COLUMN;
						action.result_type = result.type;
						action.added_column = result.column;
						action.function = nullptr;
						action.argument_names.clear();
						in.clear();
					}
				}
			}

			needed_columns.insert(in.begin(), in.end());
		}
	}

	/// Не будем выбрасывать все входные столбцы, чтобы не потерять количество строк в блоке.
	if (needed_columns.empty() && !input_columns.empty())
		needed_columns.insert(getSmallestColumn(input_columns));

	/// Не будем оставлять блок пустым, чтобы не потерять количество строк в нем.
	if (final_columns.empty())
		final_columns.insert(getSmallestColumn(input_columns));

	for (NamesAndTypesList::iterator it = input_columns.begin(); it != input_columns.end();)
	{
		NamesAndTypesList::iterator it0 = it;
		++it;
		if (!needed_columns.count(it0->name))
		{
			if (unmodified_columns.count(it0->name))
				sample_block.erase(it0->name);
			input_columns.erase(it0);
		}
	}

	for (int i = static_cast<int>(sample_block.columns()) - 1; i >= 0; --i)
	{
		const std::string & name = sample_block.getByPosition(i).name;
		if (!final_columns.count(name))
			add(ExpressionAction::removeColumn(name));
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
		if (actions[i].type == ExpressionAction::APPLY_FUNCTION)
			ss << actions[i].result_name;
		if (actions[i].type == ExpressionAction::ARRAY_JOIN)
		{
			ss << (actions[i].array_join_is_left ? "LEFT ARRAY JOIN" : "ARRAY JOIN") << "{";
			for (NameSet::const_iterator it = actions[i].array_joined_columns.begin();
				 it != actions[i].array_joined_columns.end(); ++it)
			{
				if (it != actions[i].array_joined_columns.begin())
					ss << ", ";
				ss << *it;
			}
			ss << "}";
		}

		/// TODO JOIN
	}

	ss << ": {";
	NamesAndTypesList output_columns = sample_block.getColumnsList();
	for (NamesAndTypesList::const_iterator it = output_columns.begin(); it != output_columns.end(); ++it)
	{
		if (it != output_columns.begin())
			ss << ", ";
		ss << it->name;
	}
	ss << "}";

	return ss.str();
}

std::string ExpressionActions::dumpActions() const
{
	std::stringstream ss;

	ss << "input:\n";
	for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
		ss << it->name << " " << it->type->getName() << "\n";

	ss << "\nactions:\n";
	for (size_t i = 0; i < actions.size(); ++i)
		ss << actions[i].toString() << '\n';

	ss << "\noutput:\n";
	NamesAndTypesList output_columns = sample_block.getColumnsList();
	for (NamesAndTypesList::const_iterator it = output_columns.begin(); it != output_columns.end(); ++it)
		ss << it->name << " " << it->type->getName() << "\n";

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
		if (actions[i].type == ExpressionAction::PROJECT)
			break;

		bool depends_on_array_join = false;
		Names needed;

		if (actions[i].type == ExpressionAction::ARRAY_JOIN)
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

			if (actions[i].type == ExpressionAction::REMOVE_COLUMN)
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


BlockInputStreamPtr ExpressionActions::createStreamWithNonJoinedDataIfFullOrRightJoin(size_t max_block_size) const
{
	for (const auto & action : actions)
	{
		if (action.join && (action.join->getKind() == ASTJoin::Full || action.join->getKind() == ASTJoin::Right))
		{
			Block left_sample_block;
			for (const auto & input_elem : input_columns)
				left_sample_block.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));

			return action.join->createStreamWithNonJoinedRows(left_sample_block, max_block_size);
		}
	}

	return {};
}


void ExpressionActionsChain::addStep()
{
	if (steps.empty())
		throw Exception("Cannot add action to empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

	ColumnsWithTypeAndName columns = steps.back().actions->getSampleBlock().getColumns();
	steps.push_back(Step(new ExpressionActions(columns, settings)));
}

void ExpressionActionsChain::finalize()
{
	/// Финализируем все шаги. Справа налево, чтобы определять ненужные входные столбцы.
	for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
	{
		Names required_output = steps[i].required_output;
		if (i + 1 < static_cast<int>(steps.size()))
		{
			for (const auto & it : steps[i + 1].actions->getRequiredColumnsWithTypes())
				required_output.push_back(it.name);
		}
		steps[i].actions->finalize(required_output);
	}

	/// Когда возможно, перенесем ARRAY JOIN из более ранних шагов в более поздние.
	for (size_t i = 1; i < steps.size(); ++i)
	{
		ExpressionAction action;
		if (steps[i - 1].actions->popUnusedArrayJoin(steps[i - 1].required_output, action))
			steps[i].actions->prependArrayJoin(action, steps[i - 1].actions->getSampleBlock());
	}

	/// Добавим выбрасывание ненужных столбцов в начало каждого шага.
	for (size_t i = 1; i < steps.size(); ++i)
	{
		size_t columns_from_previous = steps[i - 1].actions->getSampleBlock().columns();

		/// Если на выходе предыдущего шага образуются ненужные столбцы, добавим в начало этого шага их выбрасывание.
		/// За исключением случая, когда мы выбросим все столбцы и потеряем количество строк в блоке.
		if (!steps[i].actions->getRequiredColumnsWithTypes().empty()
			&& columns_from_previous > steps[i].actions->getRequiredColumnsWithTypes().size())
			steps[i].actions->prependProjectInput();
	}
}

std::string ExpressionActionsChain::dumpChain()
{
	std::stringstream ss;

	for (size_t i = 0; i < steps.size(); ++i)
	{
		ss << "step " << i << "\n";
		ss << "required output:\n";
		for (const std::string & name : steps[i].required_output)
			ss << name << "\n";
		ss << "\n" << steps[i].actions->dumpActions() << "\n";
	}

	return ss.str();
}

}
