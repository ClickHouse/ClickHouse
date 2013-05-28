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
		if (analyzer.appendWhere(before))
			before.addStep();
		analyzer.appendAggregateFunctionsArguments(before);
		analyzer.appendGroupBy(before);
		before.finalize();
		
		ExpressionActionsChain after;
		if (analyzer.appendHaving(after))
			after.addStep();
		analyzer.appendSelect(after);
		analyzer.appendOrderBy(after);
		after.addStep();
		analyzer.appendProjectResult(after);
		after.finalize();
		
		std::cout << "before aggregation:\n\n";
		for (size_t i = 0; i < before.steps.size(); ++i)
		{
			std::cout << before.steps[i].actions->dumpActions();
			std::cout << std::endl;
		}
		
		std::cout << "\nafter aggregation:\n\n";
		for (size_t i = 0; i < after.steps.size(); ++i)
		{
			std::cout << after.steps[i].actions->dumpActions();
			std::cout << std::endl;
		}
	}
	else
	{
		std::cout << "only consts:\n\n" << analyzer.getConstActions()->dumpActions() << "\n";
		
		if (dynamic_cast<ASTSelectQuery *>(&*root))
		{
			ExpressionActionsChain chain;
			if (analyzer.appendWhere(chain))
				chain.addStep();
			analyzer.appendSelect(chain);
			analyzer.appendOrderBy(chain);
			chain.addStep();
			analyzer.appendProjectResult(chain);
			chain.finalize();
			
			for (size_t i = 0; i < chain.steps.size(); ++i)
			{
				std::cout << chain.steps[i].actions->dumpActions();
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
