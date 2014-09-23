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
		ASTPtr makeIdentifier(const ASTPtr & expr, const Context & context)
		{
			/// for identifier just return its name
			if (typeid_cast<const ASTIdentifier *>(expr.get()))
				return expr;

			/// for string literal return its value
			if (const auto literal = typeid_cast<const ASTLiteral *>(expr.get()))
				return new ASTIdentifier{{}, safeGet<const String &>(literal->value)};

			/// otherwise evaluate expression and ensure it has string type
			Block block{};
			ExpressionAnalyzer{expr, context, { { "", new DataTypeString } }}.getActions(false)->execute(block);

			const auto & column_name_type = block.getByName(expr->getColumnName());

			if (!typeid_cast<const DataTypeString *>(column_name_type.type.get()))
				throw Exception{""};

			return new ASTIdentifier{{}, column_name_type.column->getDataAt(0).toString()};
		}

		String evaluateDatabaseName(ASTPtr & expr, const Context & context)
		{
			expr = makeIdentifier(expr, context);
			return static_cast<ASTIdentifier *>(expr.get())->name;
		}
	}
}
