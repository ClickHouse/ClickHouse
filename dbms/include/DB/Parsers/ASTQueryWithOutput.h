#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{
	
	
	/** Запрос с секцией FORMAT.
	 */
	class ASTQueryWithOutput : public IAST
	{
	public:
		ASTPtr format;
		
		ASTQueryWithOutput() {}
		ASTQueryWithOutput(StringRange range_) : IAST(range_) {}
	};
	
}
