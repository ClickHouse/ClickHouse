#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Parsers/ASTSet.h>

namespace DB
{

PKCondition::PKCondition(ASTPtr query, const Context & context_, const NamesAndTypesList & all_columns, const SortDescription & sort_descr_)
	: sort_descr(sort_descr_)
{
	for (size_t i = 0; i < sort_descr.size(); ++i)
	{
		std::string name = sort_descr[i].column_name;
		if (!pk_columns.count(name))
			pk_columns[name] = i;
	}

	/** Вычисление выражений, зависящих только от констант.
	 * Чтобы индекс мог использоваться, если написано, например WHERE Date = toDate(now()).
	 */
	ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(query, context_, all_columns).getConstActions();
	Block block_with_constants;

	/// В блоке должен быть хотя бы один столбец, чтобы у него было известно число строк.
	ColumnWithNameAndType dummy_column;
	dummy_column.name = "_dummy";
	dummy_column.type = new DataTypeUInt8;
	dummy_column.column = new ColumnConstUInt8(1, 0);
	block_with_constants.insert(dummy_column);

	expr_for_constant_folding->execute(block_with_constants);

	/// Преобразуем секцию WHERE в обратную польскую строку.
	ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*query);
	if (select.where_expression)
	{
		traverseAST(select.where_expression, block_with_constants);

		if (select.prewhere_expression)
		{
			traverseAST(select.prewhere_expression, block_with_constants);
			rpn.emplace_back(RPNElement::FUNCTION_AND);
		}
	}
	else if (select.prewhere_expression)
	{
		traverseAST(select.prewhere_expression, block_with_constants);
	}
	else
	{
		rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
	}
}

bool PKCondition::addCondition(const String & column, const Range & range)
{
	if (!pk_columns.count(column))
		return false;
	rpn.emplace_back(RPNElement::FUNCTION_IN_RANGE, pk_columns[column], range);
	rpn.emplace_back(RPNElement::FUNCTION_AND);
	return true;
}

/** Получить значение константного выражения.
 * Вернуть false, если выражение не константно.
 */
static bool getConstant(ASTPtr & expr, Block & block_with_constants, Field & value)
{
	String column_name = expr->getColumnName();

	if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(&*expr))
	{
		/// литерал
		value = lit->value;
		return true;
	}
	else if (block_with_constants.has(column_name) && block_with_constants.getByName(column_name).column->isConst())
	{
		/// выражение, вычислившееся в константу
		value = (*block_with_constants.getByName(column_name).column)[0];
		return true;
	}
	else
		return false;
}

void PKCondition::traverseAST(ASTPtr & node, Block & block_with_constants)
{
	RPNElement element;

	if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node))
	{
		if (operatorFromAST(func, element))
		{
			ASTs & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;
			for (size_t i = 0; i < args.size(); ++i)
			{
				traverseAST(args[i], block_with_constants);

				/** Первая часть условия - для корректной поддержки функций and и or произвольной арности
				  * - в этом случае добавляется n - 1 элементов (где n - количество аргументов).
				  */
				if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
					rpn.push_back(element);
			}

			return;
		}
	}

	if (!atomFromAST(node, block_with_constants, element))
	{
		element.function = RPNElement::FUNCTION_UNKNOWN;
	}

	rpn.push_back(element);
}

bool PKCondition::atomFromAST(ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
	/// Фнукции < > = != <= >= in , у которых один агрумент константа, другой - один из столбцов первичного ключа.
	if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node))
	{
		ASTs & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;

		if (args.size() != 2)
			return false;

		/// Если true, слева константа.
		bool inverted;
		size_t column;
		Field value;

		if (pk_columns.count(args[0]->getColumnName()) && getConstant(args[1], block_with_constants, value))
		{
			inverted = false;
			column = pk_columns[args[0]->getColumnName()];
		}
		else if (pk_columns.count(args[1]->getColumnName()) && getConstant(args[0], block_with_constants, value))
		{
			inverted = true;
			column = pk_columns[args[1]->getColumnName()];
		}
		else if (pk_columns.count(args[0]->getColumnName()) && typeid_cast<ASTSet *>(args[1].get()))
		{
			inverted = false;
			column = pk_columns[args[0]->getColumnName()];
		}
		else
			return false;

		std::string func_name = func->name;

		/// Заменим <const> <sign> <column> на <column> <-sign> <const>
		if (inverted)
		{
			if (func_name == "less")
				func_name = "greater";
			else if (func_name == "greater")
				func_name = "less";
			else if (func_name == "greaterOrEquals")
				func_name = "lessOrEquals";
			else if (func_name == "lessOrEquals")
				func_name = "greaterOrEquals";
		}

		out.function = RPNElement::FUNCTION_IN_RANGE;
		out.key_column = column;

		if (func_name == "notEquals")
		{
			out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
			out.range = Range(value);
		}
		else if (func_name == "equals")
			out.range = Range(value);
		else if (func_name == "less")
			out.range = Range::createRightBounded(value, false);
		else if (func_name == "greater")
			out.range = Range::createLeftBounded(value, false);
		else if (func_name == "lessOrEquals")
			out.range = Range::createRightBounded(value, true);
		else if (func_name == "greaterOrEquals")
			out.range = Range::createLeftBounded(value, true);
		else if (func_name == "in" || func_name == "notIn")
		{
			out.function = func_name == "in" ? RPNElement::FUNCTION_IN_SET : RPNElement::FUNCTION_NOT_IN_SET;
			out.in_function = node;
		}
		else
			return false;

		return true;
	}

	return false;
}

bool PKCondition::operatorFromAST(ASTFunction * func, RPNElement & out)
{
	/// Фнукции AND, OR, NOT.
	ASTs & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;

	if (func->name == "not")
	{
		if (args.size() != 1)
			return false;

		out.function = RPNElement::FUNCTION_NOT;
	}
	else
	{
		if (func->name == "and")
			out.function = RPNElement::FUNCTION_AND;
		else if (func->name == "or")
			out.function = RPNElement::FUNCTION_OR;
		else
			return false;
	}

	return true;
}

String PKCondition::toString() const
{
	String res;
	for (size_t i = 0; i < rpn.size(); ++i)
	{
		if (i)
			res += ", ";
		res += rpn[i].toString();
	}
	return res;
}

bool PKCondition::mayBeTrueInRange(const Field * left_pk, const Field * right_pk, bool right_bounded) const
{
	/// Найдем диапазоны элементов ключа.
	std::vector<Range> key_ranges(sort_descr.size(), Range());

	if (right_bounded)
	{
		for (size_t i = 0; i < sort_descr.size(); ++i)
		{
			if (left_pk[i] == right_pk[i])
			{
				key_ranges[i] = Range(left_pk[i]);
			}
			else
			{
				key_ranges[i] = Range(left_pk[i], true, right_pk[i], true);
				break;
			}
		}
	}
	else
	{
		key_ranges[0] = Range::createLeftBounded(left_pk[0], true);
	}

	std::vector<BoolMask> rpn_stack;
	for (size_t i = 0; i < rpn.size(); ++i)
	{
		const auto & element = rpn[i];
		if (element.function == RPNElement::FUNCTION_UNKNOWN)
		{
			rpn_stack.emplace_back(true, true);
		}
		else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE || element.function == RPNElement::FUNCTION_IN_RANGE)
		{
			const Range & key_range = key_ranges[element.key_column];
			bool intersects = element.range.intersectsRange(key_range);
			bool contains = element.range.containsRange(key_range);

			rpn_stack.emplace_back(intersects, !contains);
			if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
				rpn_stack.back() = !rpn_stack.back();
		}
		else if (element.function == RPNElement::FUNCTION_IN_SET || element.function == RPNElement::FUNCTION_NOT_IN_SET)
		{
			auto in_func = typeid_cast<const ASTFunction *>(element.in_function.get());
			const ASTs & args = typeid_cast<const ASTExpressionList &>(*in_func->arguments).children;
			auto ast_set = typeid_cast<const ASTSet *>(args[1].get());
			if (in_func && ast_set)
			{
				const Range & key_range = key_ranges[element.key_column];

				rpn_stack.push_back(ast_set->set->mayBeTrueInRange(key_range));
				if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
					rpn_stack.back() = !rpn_stack.back();
			}
			else
			{
				throw DB::Exception("Set for IN is not created yet!", ErrorCodes::LOGICAL_ERROR);
			}
		}
		else if (element.function == RPNElement::FUNCTION_NOT)
		{
			rpn_stack.back() = !rpn_stack.back();
		}
		else if (element.function == RPNElement::FUNCTION_AND)
		{
			auto arg1 = rpn_stack.back();
			rpn_stack.pop_back();
			auto arg2 = rpn_stack.back();
			rpn_stack.back() = arg1 & arg2;
		}
		else if (element.function == RPNElement::FUNCTION_OR)
		{
			auto arg1 = rpn_stack.back();
			rpn_stack.pop_back();
			auto arg2 = rpn_stack.back();
			rpn_stack.back() = arg1 | arg2;
		}
		else
			throw Exception("Unexpected function type in PKCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
	}

	if (rpn_stack.size() != 1)
		throw Exception("Unexpected stack size in PkCondition::mayBeTrueInRange", ErrorCodes::LOGICAL_ERROR);

	return rpn_stack[0].can_be_true;
}

bool PKCondition::mayBeTrueInRange(const Field * left_pk, const Field * right_pk) const
{
	return mayBeTrueInRange(left_pk, right_pk, true);
}

bool PKCondition::mayBeTrueAfter(const Field * left_pk) const
{
	return mayBeTrueInRange(left_pk, nullptr, false);
}

const ASTSet * PKCondition::RPNElement::inFunctionToSet() const
{
	auto in_func = typeid_cast<const ASTFunction *>(in_function.get());
	if (!in_func)
		return nullptr;
	const ASTs & args = typeid_cast<const ASTExpressionList &>(*in_func->arguments).children;
	auto ast_set = typeid_cast<const ASTSet *>(args[1].get());
	return ast_set;
}

String PKCondition::RPNElement::toString() const
{
	std::ostringstream ss;
	switch (function)
	{
		case FUNCTION_AND:
			return "and";
		case FUNCTION_OR:
			return "or";
		case FUNCTION_NOT:
			return "not";
		case FUNCTION_UNKNOWN:
			return "unknown";
		case FUNCTION_NOT_IN_SET:
		case FUNCTION_IN_SET:
		{
			ss << "(column " << key_column << (function == FUNCTION_IN_SET ? " in " : " notIn ") << inFunctionToSet()->set->describe() << ")";
			return ss.str();
		}
		case FUNCTION_IN_RANGE:
		case FUNCTION_NOT_IN_RANGE:
		{
			ss << "(column " << key_column << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString() << ")";
			return ss.str();
		}
		default:
			return "ERROR";
	}
}


bool PKCondition::alwaysUnknown() const
{
	std::vector<UInt8> rpn_stack;

	for (size_t i = 0; i < rpn.size(); ++i)
	{
		const auto & element = rpn[i];

		if (element.function == RPNElement::FUNCTION_UNKNOWN)
		{
			rpn_stack.push_back(true);
		}
		else if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE
			|| element.function == RPNElement::FUNCTION_IN_RANGE
			|| element.function == RPNElement::FUNCTION_IN_SET
			|| element.function == RPNElement::FUNCTION_NOT_IN_SET)
		{
			rpn_stack.push_back(false);
		}
		else if (element.function == RPNElement::FUNCTION_NOT)
		{
		}
		else if (element.function == RPNElement::FUNCTION_AND)
		{
			auto arg1 = rpn_stack.back();
			rpn_stack.pop_back();
			auto arg2 = rpn_stack.back();
			rpn_stack.back() = arg1 & arg2;
		}
		else if (element.function == RPNElement::FUNCTION_OR)
		{
			auto arg1 = rpn_stack.back();
			rpn_stack.pop_back();
			auto arg2 = rpn_stack.back();
			rpn_stack.back() = arg1 | arg2;
		}
		else
			throw Exception("Unexpected function type in PKCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
	}

	return rpn_stack[0];
}


}
