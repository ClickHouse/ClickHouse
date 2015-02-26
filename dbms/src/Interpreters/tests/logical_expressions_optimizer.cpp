#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/queryToString.h>
#include <DB/Interpreters/LogicalExpressionsOptimizer.h>
#include <DB/Interpreters/Settings.h>

#include <iostream>
#include <deque>
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
	using namespace DB;

	auto & children = ast->children;
	if (children.empty())
		return;

	for (auto & child : children)
		reorder(&*child);

	std::sort(children.begin(), children.end(), [](const ASTPtr & lhs, const ASTPtr & rhs)
	{
		return lhs->getTreeID() < rhs->getTreeID();
	});
}

bool equals(const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
{
	using namespace DB;

	ASTPtr lhs_reordered = lhs->clone();
	reorder(&*lhs_reordered);

	ASTPtr rhs_reordered = rhs->clone();
	reorder(&*rhs_reordered);

	std::deque<const IAST *> to_visit_left;
	std::deque<const IAST *> to_visit_right;

	to_visit_left.push_back(&*lhs_reordered);
	to_visit_right.push_back(&*rhs_reordered);

	while (!to_visit_left.empty())
	{
		auto node_left = to_visit_left.back();
		to_visit_left.pop_back();

		if (to_visit_right.empty())
			return false;

		auto node_right = to_visit_right.back();
		to_visit_right.pop_back();

		if (node_left->getTreeID() != node_right->getTreeID())
			return false;

		if (node_left->children.size() != node_right->children.size())
			return false;

		for (auto & child : node_left->children)
			to_visit_left.push_back(&*child);
		for (auto & child : node_right->children)
			to_visit_right.push_back(&*child);
	}

	return true;
}

bool parse(DB::ASTPtr  & ast, const std::string & query)
{
	using namespace DB;

	ParserSelectQuery parser;
	const char * pos = &query[0];
	const char * end = &query[0] + query.size();

	Expected expected = "";
	return parser.parse(pos, end, ast, expected);
}

TestResult check(const TestEntry & entry)
{
	using namespace DB;

	try
	{
		ASTPtr ast_input;
		if (!parse(ast_input, entry.input))
			return TestResult(false, "parse error");

		auto select_query = typeid_cast<ASTSelectQuery *>(&*ast_input);

		Settings settings;
		settings.optimize_min_equality_disjunction_chain_length = entry.limit;

		LogicalExpressionsOptimizer optimizer(select_query, settings);
		optimizer.optimizeDisjunctiveEqualityChains();

		ASTPtr ast_expected;
		if (!parse(ast_expected, entry.expected_output))
			return TestResult(false, "parse error");

		bool res = equals(ast_input, ast_expected);
		std::string output = queryToString(ast_input);

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
