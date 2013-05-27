#include <DB/Interpreters/ExpressionActions.h>
#include <set>

namespace DB
{

void ExpressionActions::Action::prepare(Block & sample_block)
{
	if (type == REMOVE_COLUMN || type == COPY_COLUMN)
		if (!sample_block.has(source_name))
			throw Exception("Not found column '" + source_name + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
		
	if (type != REMOVE_COLUMN)
		if (sample_block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
	
	ColumnWithNameAndType new_column;
	
	if (type == APPLY_FUNCTION)
	{
		DataTypes types(argument_names.size());
		for (size_t i = 0; i < argument_names.size(); ++i)
		{
			if (!sample_block.has(argument_names[i]))
				throw Exception("Unknown identifier: '" + argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
			types[i] = sample_block.getByName(argument_names[i]).type;
		}
		
		result_type = function->getReturnType(types);
		
		new_column.column = result_type->createConstColumn(1, result_type->getDefault());
	}
	else if (type == ADD_COLUMN)
	{
		new_column.column = added_column;
	}
	else if (type == COPY_COLUMN)
	{
		new_column = sample_block.getByName(source_name);
		result_type = new_column.type;
	}
	
	if (type != REMOVE_COLUMN)
	{
		new_column.name = result_name;
		new_column.type = result_type;
		sample_block.insert(new_column);
	}
	else
	{
		sample_block.erase(source_name);
	}
}

void ExpressionActions::Action::execute(Block & block)
{
	if (type == REMOVE_COLUMN || type == COPY_COLUMN)
		if (!block.has(source_name))
			throw Exception("Not found column '" + source_name + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
	
	if (type != REMOVE_COLUMN)
		if (block.has(result_name))
			throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
	
	if (type == APPLY_FUNCTION)
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
		
		return;
	}
	
	if (type == REMOVE_COLUMN)
	{
		block.erase(source_name);
	}
	else
	{
		ColumnWithNameAndType new_column;
		
		if (type == ADD_COLUMN)
		{
			new_column.column = added_column->cloneResized(block.rows());
		}
		else if (type == COPY_COLUMN)
		{
			new_column = block.getByName(source_name);
			result_type = new_column.type;
		}
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
		default:
			throw Exception("Unexpected Action type", ErrorCodes::LOGICAL_ERROR);
	}
	
	return ss.str();
}

void ExpressionActions::finalize(const NamesWithAliases & output_columns)
{
	typedef std::set<std::string> NameSet;
	
	NameSet final_columns;
	for (size_t i = 0; i < output_columns.size(); ++i)
	{
		const std::string name = output_columns[i].first;
		const std::string alias = output_columns[i].second;
		if (!sample_block.has(name))
			throw Exception("Unknown column: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
		if (alias != "" && alias != name)
		{
			final_columns.insert(alias);
			add(Action(name, alias));
			add(Action(name));
		}
		else
		{
			final_columns.insert(name);
		}
	}
	
	NameSet used_columns = final_columns;
	
	for (size_t i = 0; i < actions.size(); ++i)
	{
		used_columns.insert(actions[i].source_name);
		for (size_t j = 0; j < actions[i].argument_names.size(); ++j)
		{
			used_columns.insert(actions[i].argument_names[j]);
		}
	}
	for (NamesAndTypesList::iterator it = input_columns.begin(); it != input_columns.end();)
	{
		NamesAndTypesList::iterator it0 = it;
		++it;
		if (!used_columns.count(it0->first))
		{
			sample_block.erase(it0->first);
			input_columns.erase(it0);
		}
	}
	
	for (int i = static_cast<int>(sample_block.columns()) - 1; i >= 0; --i)
	{
		const std::string & name = sample_block.getByPosition(i).name;
		if (!final_columns.count(name))
			add(Action(name));
	}
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
