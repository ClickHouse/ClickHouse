#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ExpressionListParsers.h>


int main(int argc, char ** argv)
{
	using namespace DB;

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
		col.name = argv[i];
		col.type = context.getDataTypeFactory().get(argv[i + 1]);
		columns.push_back(col);
	}

	ASTPtr root;
	ParserPtr parsers[] = {ParserPtr(new ParserSelectQuery), ParserPtr(new ParserExpressionList)};
	for (size_t i = 0; i < sizeof(parsers)/sizeof(parsers[0]); ++i)
	{
		IParser & parser = *parsers[i];
		const char * pos = argv[1];
		const char * end = argv[1] + strlen(argv[1]);
		const char * max_parsed_pos = pos;
		Expected expected = "";
		if (parser.parse(pos, end, root, max_parsed_pos, expected))
			break;
		else
			root = nullptr;
	}
	if (!root)
	{
		std::cerr << "invalid expression (should be select query or expression list)" << std::endl;
		return 2;
	}

	formatAST(*root, std::cout);
	std::cout << std::endl;

	ExpressionAnalyzer analyzer(root, context, columns);

	Names required = analyzer.getRequiredColumns();
	std::cout << "required columns:\n";
	for (size_t i = 0; i < required.size(); ++i)
	{
		std::cout << required[i] << "\n";
	}
	std::cout << "\n";

	std::cout << "only consts:\n\n" << analyzer.getConstActions()->dumpActions() << "\n";

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
		if (analyzer.appendWhere(before, false))
			before.addStep();
		analyzer.appendAggregateFunctionsArguments(before, false);
		analyzer.appendGroupBy(before, false);
		before.finalize();

		ExpressionActionsChain after;
		if (analyzer.appendHaving(after, false))
			after.addStep();
		analyzer.appendSelect(after, false);
		analyzer.appendOrderBy(after, false);
		after.addStep();
		analyzer.appendProjectResult(after, false);
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
		if (typeid_cast<ASTSelectQuery *>(&*root))
		{
			ExpressionActionsChain chain;
			if (analyzer.appendWhere(chain, false))
				chain.addStep();
			analyzer.appendSelect(chain, false);
			analyzer.appendOrderBy(chain, false);
			chain.addStep();
			analyzer.appendProjectResult(chain, false);
			chain.finalize();

			for (size_t i = 0; i < chain.steps.size(); ++i)
			{
				std::cout << chain.steps[i].actions->dumpActions();
				std::cout << std::endl;
			}
		}
		else
		{
			std::cout << "unprojected actions:\n\n" << analyzer.getActions(false)->dumpActions() << "\n";
			std::cout << "projected actions:\n\n" << analyzer.getActions(true)->dumpActions() << "\n";
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
