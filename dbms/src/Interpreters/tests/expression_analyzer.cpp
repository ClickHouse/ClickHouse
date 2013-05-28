#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>

using namespace DB;


int main(int argc, char ** argv)
{
	try
	{
	
	if (argc < 2)
	{
		std::cerr << "at least 1 argument expected" << std::endl;
		return 1;
	}
	
	Context context;
	
	NamesAndTypesList columns;
	for (int i = 2; i + 1 < argc; i += 2)
	{
		NameAndTypePair col;
		col.first = argv[i];
		col.second = context.getDataTypeFactory().get(argv[i + 1]);
		columns.push_back(col);
	}
	
	ParserSelectQuery parser;
	const char * pos = argv[1];
	const char * end = argv[1] + strlen(argv[1]);
	ASTPtr root;
	std::string expected;
	if (!parser.parse(pos ,end, root, expected))
	{
		std::cerr << "expected " << expected << std::endl;
		return 2;
	}
	
	formatAST(*root, std::cout);
	std::cout << std::endl;
	
	ExpressionAnalyzer analyzer(root, context, columns);
	
	/// До агрегации:
	bool appendWhere(ExpressionActionsChain & chain);
	bool appendGroupBy(ExpressionActionsChain & chain);
	bool appendAggregateFunctionsArguments(ExpressionActionsChain & chain);
	/// Финализирует всю цепочку.
	void appendProjectBeforeAggregation(ExpressionActionsChain & chain);
	
	/// После агрегации:
	bool appendHaving(ExpressionActionsChain & chain);
	void appendSelect(ExpressionActionsChain & chain);
	bool appendOrderBy(ExpressionActionsChain & chain);
	/// Действия, удаляющие из блока столбцы, кроме столбцов из указанных секций запроса.
	/// Столбцы из секции SELECT также переупорядочиваются и переименовываются в алиасы.
	
	if (analyzer.hasAggregation())
	{
		Names key_names;
		AggregateDescriptions aggregates;
		analyzer.getAggregateInfo(key_names, aggregates);
		
		std::cout << "keys:\n";
		for (size_t i = 0; i < key_names.size(); ++i)
			std::cout << key_names[i] << "\n";
		std::cout << "\n";
		
		std::cout << "aggregates:\n";
		for (size_t i = 0; i < aggregates.size(); ++i)
		{
			AggregateDescription desc = aggregates[i];
			
			std::cout << desc.column_name << " = " << desc.function->getName() << " ( ";
			for (size_t j = 0; j < desc.argument_names.size(); ++j)
				std::cout << desc.argument_names[j] << " ";
			std::cout << ")\n";
		}
		std::cout << "\n";
		
		ExpressionActionsChain before;
		analyzer.appendWhere(before);
		analyzer.appendAggregateFunctionsArguments(before);
		analyzer.appendGroupBy(before);
		analyzer.appendProjectBeforeAggregation(before);
		
		ExpressionActionsChain after;
		analyzer.appendHaving(after);
		analyzer.appendSelect(after);
		analyzer.appendOrderBy(after);
		analyzer.appendProject(after, true, true);
		analyzer.appendProject(after, true, false);
		
		std::cout << "before aggregation:\n\n";
		for (size_t i = 0; i < before.size(); ++i)
		{
			before[i]->dumpActions();
			std::cout << std::endl;
		}
		
		std::cout << "\nafter aggregation:\n\n";
		for (size_t i = 0; i < after.size(); ++i)
		{
			after[i]->dumpActions();
			std::cout << std::endl;
		}
	}
	else
	{
		std::cout << "only consts:\n\n" << analyzer.getConstActions()->dumpActions() << "\n";
		
		if (dynamic_cast<ASTSelectQuery *>(&*root))
		{
			ExpressionActionsChain chain;
			analyzer.appendWhere(chain);
			analyzer.appendSelect(chain);
			analyzer.appendOrderBy(chain);
			analyzer.appendProject(chain, true, true);
			analyzer.appendProject(chain, true, false);
			
			for (size_t i = 0; i < chain.size(); ++i)
			{
				chain[i]->dumpActions();
				std::cout << std::endl;
			}
		}
		else
		{
			std::cout << analyzer.getActions()->dumpActions() << "\n";
		}
	}
	
	}
	catch (Exception & e)
	{
		std::cerr << "Exception " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString();
		return 3;
	}
	
	return 0;
}
