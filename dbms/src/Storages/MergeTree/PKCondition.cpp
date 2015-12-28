#include <DB/Storages/MergeTree/PKCondition.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Parsers/ASTSet.h>
#include <DB/Functions/FunctionFactory.h>


namespace DB
{


const PKCondition::AtomMap PKCondition::atom_map{
	{
		"notEquals",
		[] (RPNElement & out, const Field & value, ASTPtr &)
		{
			out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
			out.range = Range(value);
		}
	},
	{
		"equals",
		[] (RPNElement & out, const Field & value, ASTPtr &)
		{
			out.function = RPNElement::FUNCTION_IN_RANGE;
			out.range = Range(value);
		}
	},
	{
		"less",
		[] (RPNElement & out, const Field & value, ASTPtr &)
		{
			out.function = RPNElement::FUNCTION_IN_RANGE;
			out.range = Range::createRightBounded(value, false);
		}
	},
	{
		"greater",
		[] (RPNElement & out, const Field & value, ASTPtr &)
		{
			out.function = RPNElement::FUNCTION_IN_RANGE;
			out.range = Range::createLeftBounded(value, false);
		}
	},
	{
		"lessOrEquals",
		[] (RPNElement & out, const Field & value, ASTPtr &)
		{
			out.function = RPNElement::FUNCTION_IN_RANGE;
			out.range = Range::createRightBounded(value, true);
		}
	},
	{
		"greaterOrEquals",
		[] (RPNElement & out, const Field & value, ASTPtr &)
		{
			out.function = RPNElement::FUNCTION_IN_RANGE;
			out.range = Range::createLeftBounded(value, true);
		}
	},
	{
		"in",
		[] (RPNElement & out, const Field & value, ASTPtr & node)
		{
			out.function = RPNElement::FUNCTION_IN_SET;
			out.in_function = node;
		}
	},
	{
		"notIn",
		[] (RPNElement & out, const Field & value, ASTPtr & node)
		{
			out.function = RPNElement::FUNCTION_NOT_IN_SET;
			out.in_function = node;
		}
	}
};


inline bool Range::equals(const Field & lhs, const Field & rhs) { return apply_visitor(FieldVisitorAccurateEquals(), lhs, rhs); }
inline bool Range::less(const Field & lhs, const Field & rhs) { return apply_visitor(FieldVisitorAccurateLess(), lhs, rhs); }


Block PKCondition::getBlockWithConstants(
	const ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns)
{
	Block result
	{
		{ new ColumnConstUInt8{1, 0}, new DataTypeUInt8, "_dummy" }
	};

	const auto expr_for_constant_folding = ExpressionAnalyzer{query, context, nullptr, all_columns}
		.getConstActions();

	expr_for_constant_folding->execute(result);

	return result;
}


PKCondition::PKCondition(ASTPtr & query, const Context & context, const NamesAndTypesList & all_columns, const SortDescription & sort_descr_)
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
	Block block_with_constants = getBlockWithConstants(query, context, all_columns);

	/// Преобразуем секцию WHERE в обратную польскую строку.
	ASTSelectQuery & select = typeid_cast<ASTSelectQuery &>(*query);
	if (select.where_expression)
	{
		traverseAST(select.where_expression, context, block_with_constants);

		if (select.prewhere_expression)
		{
			traverseAST(select.prewhere_expression, context, block_with_constants);
			rpn.emplace_back(RPNElement::FUNCTION_AND);
		}
	}
	else if (select.prewhere_expression)
	{
		traverseAST(select.prewhere_expression, context, block_with_constants);
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
static bool getConstant(const ASTPtr & expr, Block & block_with_constants, Field & value)
{
	String column_name = expr->getColumnName();

	if (const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(&*expr))
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

void PKCondition::traverseAST(ASTPtr & node, const Context & context, Block & block_with_constants)
{
	RPNElement element;

	if (ASTFunction * func = typeid_cast<ASTFunction *>(&*node))
	{
		if (operatorFromAST(func, element))
		{
			auto & args = typeid_cast<ASTExpressionList &>(*func->arguments).children;
			for (size_t i = 0, size = args.size(); i < size; ++i)
			{
				traverseAST(args[i], context, block_with_constants);

				/** Первая часть условия - для корректной поддержки функций and и or произвольной арности
				  * - в этом случае добавляется n - 1 элементов (где n - количество аргументов).
				  */
				if (i != 0 || element.function == RPNElement::FUNCTION_NOT)
					rpn.push_back(element);
			}

			return;
		}
	}

	if (!atomFromAST(node, context, block_with_constants, element))
	{
		element.function = RPNElement::FUNCTION_UNKNOWN;
	}

	rpn.push_back(element);
}


bool PKCondition::isPrimaryKeyPossiblyWrappedByMonotonicFunctions(
	const ASTPtr & node,
	const Context & context,
	size_t & out_primary_key_column_num,
	RPNElement::MonotonicFunctionsChain & out_functions_chain)
{
	std::vector<const ASTFunction *> chain_not_tested_for_monotonicity;

	if (!isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_primary_key_column_num, chain_not_tested_for_monotonicity))
		return false;

	for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
	{
		FunctionPtr func = FunctionFactory::instance().tryGet((*it)->name, context);
		if (!func || !func->hasInformationAboutMonotonicity())
			return false;

		out_functions_chain.push_back(func);
	}

	return true;
}


bool PKCondition::isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(
	const ASTPtr & node,
	size_t & out_primary_key_column_num,
	std::vector<const ASTFunction *> & out_functions_chain)
{
	/** Сам по себе, столбец первичного ключа может быть функциональным выражением. Например, intHash32(UserID).
	  * Поэтому, используем полное имя выражения для поиска.
	  */
	String name = node->getColumnName();

	auto it = pk_columns.find(name);
	if (pk_columns.end() != it)
	{
		out_primary_key_column_num = it->second;
		return true;
	}

	if (const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get()))
	{
		const auto & args = func->arguments->children;
		if (args.size() != 1)
			return false;

		out_functions_chain.push_back(func);

		if (!isPrimaryKeyPossiblyWrappedByMonotonicFunctionsImpl(args[0], out_primary_key_column_num, out_functions_chain))
			return false;

		return true;
	}

	return false;
}


bool PKCondition::atomFromAST(ASTPtr & node, const Context & context, Block & block_with_constants, RPNElement & out)
{
	/** Функции < > = != <= >= in notIn, у которых один агрумент константа, другой - один из столбцов первичного ключа,
	  *  либо он же, завёрнутый в цепочку возможно-монотонных функций.
	  */
	if (const ASTFunction * func = typeid_cast<const ASTFunction *>(&*node))
	{
		const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

		if (args.size() != 2)
			return false;

		/// Если true, слева константа.
		bool inverted;
		size_t column;
		Field value;
		RPNElement::MonotonicFunctionsChain chain;

		if (getConstant(args[1], block_with_constants, value) && isPrimaryKeyPossiblyWrappedByMonotonicFunctions(args[0], context, column, chain))
		{
			inverted = false;
		}
		else if (getConstant(args[0], block_with_constants, value) && isPrimaryKeyPossiblyWrappedByMonotonicFunctions(args[1], context, column, chain))
		{
			inverted = true;
		}
		else if (typeid_cast<const ASTSet *>(args[1].get()) && isPrimaryKeyPossiblyWrappedByMonotonicFunctions(args[0], context, column, chain))
		{
			inverted = false;
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
			else if (func_name == "in" || func_name == "notIn")
			{
				/// const IN x не имеет смысла (в отличие от x IN const).
				return false;
			}
		}

		out.key_column = column;
		out.monotonic_functions_chain = std::move(chain);

		const auto atom_it = atom_map.find(func_name);
		if (atom_it == std::end(atom_map))
			return false;

		atom_it->second(out, value, node);

		return true;
	}

	return false;
}

bool PKCondition::operatorFromAST(const ASTFunction * func, RPNElement & out)
{
	/// Функции AND, OR, NOT.
	const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

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


static void applyFunction(
	FunctionPtr & func,
	const DataTypePtr & arg_type, const Field & arg_value,
	DataTypePtr & res_type, Field & res_value)
{
	res_type = func->getReturnType({arg_type});

	Block block
	{
		{ arg_type->createConstColumn(1, arg_value), arg_type, "x" },
		{ nullptr, res_type, "y" }
	};

	func->execute(block, {0}, 1);

	block.getByPosition(1).column->get(0, res_value);
}


bool PKCondition::mayBeTrueInRange(const Field * left_pk, const Field * right_pk, const DataTypes & data_types, bool right_bounded) const
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
		else if (element.function == RPNElement::FUNCTION_IN_RANGE
			|| element.function == RPNElement::FUNCTION_NOT_IN_RANGE
			|| element.function == RPNElement::FUNCTION_IN_SET
			|| element.function == RPNElement::FUNCTION_NOT_IN_SET)
		{
			const Range * key_range = &key_ranges[element.key_column];

			/// Случай, когда столбец обёрнут в цепочку возможно-монотонных функций.
			Range key_range_transformed;
			bool evaluation_is_not_possible = false;
			if (!element.monotonic_functions_chain.empty())
			{
				key_range_transformed = *key_range;
				DataTypePtr current_type = data_types[element.key_column];
				for (auto & func : element.monotonic_functions_chain)
				{
					/// Проверяем монотонность каждой функции на конкретном диапазоне.
					IFunction::Monotonicity monotonicity = func->getMonotonicityForRange(
						*current_type.get(), key_range_transformed.left, key_range_transformed.right);

				/*	std::cerr << "Function " << func->getName() << " is " << (monotonicity.is_monotonic ? "" : "not ")
						<< "monotonic " << (monotonicity.is_monotonic ? (monotonicity.is_positive ? "(positive) " : "(negative) ") : "")
						<< "in range "
						<< "[" << apply_visitor(FieldVisitorToString(), key_range_transformed.left)
						<< ", " << apply_visitor(FieldVisitorToString(), key_range_transformed.right) << "]\n";*/

					if (!monotonicity.is_monotonic)
					{
						evaluation_is_not_possible = true;
						break;
					}

					/// Вычисляем функцию.
					DataTypePtr new_type;
					if (!key_range_transformed.left.isNull())
						applyFunction(func, current_type, key_range_transformed.left, new_type, key_range_transformed.left);
					if (!key_range_transformed.right.isNull())
						applyFunction(func, current_type, key_range_transformed.right, new_type, key_range_transformed.right);

					if (!new_type)
					{
						evaluation_is_not_possible = true;
						break;
					}

					current_type.swap(new_type);

					if (!monotonicity.is_positive)
						key_range_transformed.swapLeftAndRight();
				}

				if (evaluation_is_not_possible)
				{
					rpn_stack.emplace_back(true, true);
					continue;
				}

				key_range = &key_range_transformed;
			}

			if (element.function == RPNElement::FUNCTION_IN_RANGE
				|| element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
			{
				bool intersects = element.range.intersectsRange(*key_range);
				bool contains = element.range.containsRange(*key_range);

				rpn_stack.emplace_back(intersects, !contains);
				if (element.function == RPNElement::FUNCTION_NOT_IN_RANGE)
					rpn_stack.back() = !rpn_stack.back();
			}
			else	/// Set
			{
				auto in_func = typeid_cast<const ASTFunction *>(element.in_function.get());
				const ASTs & args = typeid_cast<const ASTExpressionList &>(*in_func->arguments).children;
				auto ast_set = typeid_cast<const ASTSet *>(args[1].get());
				if (in_func && ast_set)
				{
					rpn_stack.push_back(ast_set->set->mayBeTrueInRange(*key_range));
					if (element.function == RPNElement::FUNCTION_NOT_IN_SET)
						rpn_stack.back() = !rpn_stack.back();
				}
				else
				{
					throw DB::Exception("Set for IN is not created yet!", ErrorCodes::LOGICAL_ERROR);
				}
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

bool PKCondition::mayBeTrueInRange(const Field * left_pk, const Field * right_pk, const DataTypes & data_types) const
{
	return mayBeTrueInRange(left_pk, right_pk, data_types, true);
}

bool PKCondition::mayBeTrueAfter(const Field * left_pk, const DataTypes & data_types) const
{
	return mayBeTrueInRange(left_pk, nullptr, data_types, false);
}

static const ASTSet & inFunctionToSet(const ASTPtr & in_function)
{
	const auto & in_func = typeid_cast<const ASTFunction &>(*in_function);
	const auto & args = typeid_cast<const ASTExpressionList &>(*in_func.arguments).children;
	const auto & ast_set = typeid_cast<const ASTSet &>(*args[1]);
	return ast_set;
}

String PKCondition::RPNElement::toString() const
{
	auto print_wrapped_column = [this](std::ostringstream & ss)
	{
		for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
			ss << (*it)->getName() << "(";

		ss << "column " << key_column;

		for (auto it = monotonic_functions_chain.rbegin(); it != monotonic_functions_chain.rend(); ++it)
			ss << ")";
	};

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
			ss << "(";
			print_wrapped_column(ss);
			ss << (function == FUNCTION_IN_SET ? " in " : " notIn ") << inFunctionToSet(in_function).set->describe();
			ss << ")";
			return ss.str();
		}
		case FUNCTION_IN_RANGE:
		case FUNCTION_NOT_IN_RANGE:
		{
			ss << "(";
			print_wrapped_column(ss);
			ss << (function == FUNCTION_NOT_IN_RANGE ? " not" : "") << " in " << range.toString();
			ss << ")";
			return ss.str();
		}
		default:
			throw Exception("Unknown function in RPNElement", ErrorCodes::LOGICAL_ERROR);
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
