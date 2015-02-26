#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/queryToString.h>
#include <DB/Interpreters/LogicalExpressionsOptimizer.h>
#include <DB/Interpreters/Settings.h>

#include <iostream>
#include <vector>
#include <utility>
#include <string>

namespace
{

struct TestEntry
{
	std::string input;
	std::string expected_output;
	UInt64 limit;
};

using TestEntries = std::vector<TestEntry>;
using TestResult = std::pair<bool, std::string>;

void reorder(DB::IAST * ast)
{
	auto & children = ast->children;
	if (children.empty())
		return;

	for (auto & child : children)
		reorder(&*child);

	std::sort(children.begin(), children.end(), [](const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
	{
		return lhs->getTreeID() < rhs->getTreeID();
	});
}

bool equals(const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
{
	DB::ASTPtr lhs_reordered = lhs->clone();
	reorder(&*lhs_reordered);

	DB::ASTPtr rhs_reordered = rhs->clone();
	reorder(&*rhs_reordered);

	return lhs_reordered->getTreeID() == rhs_reordered->getTreeID();
}

bool parse(DB::ASTPtr  & ast, const std::string & query)
{
	DB::ParserSelectQuery parser;
	const char * pos = &query[0];
	const char * end = &query[0] + query.size();

	DB::Expected expected = "";
	return parser.parse(pos, end, ast, expected);
}

TestResult check(const TestEntry & entry)
{
	try
	{
		DB::ASTPtr ast_input;
		if (!parse(ast_input, entry.input))
			return TestResult(false, "parse error");

		auto select_query = typeid_cast<DB::ASTSelectQuery *>(&*ast_input);

		DB::Settings settings;
		settings.optimize_min_equality_disjunction_chain_length = entry.limit;

		DB::LogicalExpressionsOptimizer optimizer(select_query, settings);
		optimizer.optimizeDisjunctiveEqualityChains();

		DB::ASTPtr ast_expected;
		if (!parse(ast_expected, entry.expected_output))
			return TestResult(false, "parse error");

		bool res = equals(ast_input, ast_expected);
		std::string output = DB::queryToString(ast_input);

		return TestResult(res, output);
	}
	catch (DB::Exception & e)
	{
		return TestResult(false, e.displayText());
	}
}

void performTests(const TestEntries & entries)
{
	unsigned int count = 0;
	unsigned int i = 1;
	for (const auto & entry : entries)
	{
		auto res = check(entry);
		if (res.first)
		{
			++count;
			std::cout << "Test " << i << " passed.\n";
		}
		else
			std::cout << "Test " << i << " failed. Expected: " << entry.expected_output << ". Received: " << res.second << "\n";

		++i;
	}
	std::cout << count << " out of " << entries.size() << " test(s) passed.\n";
}

void run()
{
	TestEntries entries =
	{
		{
			"SELECT 1",
			"SELECT 1",
			3
		},

		// WHERE

		{
			"SELECT name, value FROM report WHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')", 
			"SELECT name, value FROM report WHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')",
			4
		},

		{
			"SELECT name, value FROM report WHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')", 
			"SELECT name, value FROM report WHERE name IN ('Alice', 'Bob', 'Carol')",
			3
		},

		{
			"SELECT name, value FROM report WHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')", 
			"SELECT name, value FROM report WHERE name IN ('Alice', 'Bob', 'Carol')",
			2
		},

		{
			"SELECT name, value FROM report WHERE (name = 'Alice') OR (value = 1000) OR (name = 'Bob') OR (name = 'Carol')",
			"SELECT name, value FROM report WHERE (value = 1000) OR name IN ('Alice', 'Bob', 'Carol')",
			2
		},

		{
			"SELECT name, value FROM report WHERE (name = 'Alice') OR (value = 1000) OR (name = 'Bob') OR (name = 'Carol') OR (value = 2000)",
			"SELECT name, value FROM report WHERE name IN ('Alice', 'Bob', 'Carol') OR value IN (1000, 2000)",
			2
		},

		{
			"SELECT value FROM report WHERE ((value + 1) = 1000) OR ((2 * value) = 2000) OR ((2 * value) = 4000) OR ((value + 1) = 3000)",
			"SELECT value FROM report WHERE (value + 1) IN (1000, 3000) OR (2 * value) IN (2000, 4000)",
			2
		},

		// PREWHERE

		{
			"SELECT name, value FROM report PREWHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')", 
			"SELECT name, value FROM report PREWHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')",
			4
		},

		{
			"SELECT name, value FROM report PREWHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')", 
			"SELECT name, value FROM report PREWHERE name IN ('Alice', 'Bob', 'Carol')",
			3
		},

		{
			"SELECT name, value FROM report PREWHERE (name = 'Alice') OR (name = 'Bob') OR (name = 'Carol')", 
			"SELECT name, value FROM report PREWHERE name IN ('Alice', 'Bob', 'Carol')",
			2
		},

		{
			"SELECT name, value FROM report PREWHERE (name = 'Alice') OR (value = 1000) OR (name = 'Bob') OR (name = 'Carol')",
			"SELECT name, value FROM report PREWHERE (value = 1000) OR name IN ('Alice', 'Bob', 'Carol')",
			2
		},

		{
			"SELECT name, value FROM report PREWHERE (name = 'Alice') OR (value = 1000) OR (name = 'Bob') OR (name = 'Carol') OR (value = 2000)",
			"SELECT name, value FROM report PREWHERE name IN ('Alice', 'Bob', 'Carol') OR value IN (1000, 2000)",
			2
		},

		{
			"SELECT value FROM report PREWHERE ((value + 1) = 1000) OR ((2 * value) = 2000) OR ((2 * value) = 4000) OR ((value + 1) = 3000)",
			"SELECT value FROM report PREWHERE (value + 1) IN (1000, 3000) OR (2 * value) IN (2000, 4000)",
			2
		}

		// HAVING

		// XXX TODO

		// PREWHERE + WHERE + HAVING

		// XXX TODO
	};

	performTests(entries);
}

}

int main()
{
	run();
	return 0;
}
