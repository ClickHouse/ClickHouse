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
		
		std::cout << "before aggregation:\n\n" << analyzer.getActionsBeforeAggregation()->dumpActions() << "\n";
		std::cout << "after aggregation:\n\n" << analyzer.getActionsAfterAggregation()->dumpActions();
	}
	else
	{
		std::cout << "only consts:\n\n" << analyzer.getConstActions()->dumpActions() >> "\n";
		std::cout << "everything:\n\n" << analyzer.getActions()->dumpActions();
	}
	
	}
	catch (Exception & e)
	{
		std::cerr << "Exception " << e.what() << ": " << e.displayText() << "\n" << e.getStackTrace().toString();
		return 3;
	}
	
	return 0;
}
