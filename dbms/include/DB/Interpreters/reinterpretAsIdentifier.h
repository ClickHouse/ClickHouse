#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>


namespace DB
{
	namespace
	{
		ASTPtr reinterpretAsIdentifierImpl(const ASTPtr & expr, const Context & context)
		{
			/// for string literal return its value
			if (const auto literal = typeid_cast<const ASTLiteral *>(expr.get()))
				return new ASTIdentifier{{}, safeGet<const String &>(literal->value)};

			/// otherwise evaluate the expression
			Block block{};
			/** pass a dummy column name because ExpressionAnalyzer
		     *  does not work with no columns so far. */
			ExpressionAnalyzer{
				expr, context, {},
				{ { "", new DataTypeString } }
			}.getActions(false)->execute(block);

			const auto & column_name_type = block.getByName(expr->getColumnName());

			/// ensure the result of evaluation has String type
			if (!typeid_cast<const DataTypeString *>(column_name_type.type.get()))
				throw Exception{"Expression must evaluate to a String"};

			return new ASTIdentifier{{}, column_name_type.column->getDataAt(0).toString()};
		}
	}

	/** \brief if `expr` is not already ASTIdentifier evaluates it
	 *  and replaces by a new ASTIdentifier with the result of evaluation as its name.
	 *  `expr` must evaluate to a String type */
	inline ASTIdentifier & reinterpretAsIdentifier(ASTPtr & expr, const Context & context)
	{
		/// for identifier just return its name
		if (!typeid_cast<const ASTIdentifier *>(expr.get()))
			expr = reinterpretAsIdentifierImpl(expr, context);

		return static_cast<ASTIdentifier &>(*expr);
	}
}
