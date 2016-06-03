#include <DB/Parsers/IParserBase.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


bool IParserBase::parse(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	Pos begin = pos;
	Pos new_max_parsed_pos = pos;
	Expected new_expected = getName();

	bool res = parseImpl(pos, end, node, new_max_parsed_pos, new_expected);

	if (pos > new_max_parsed_pos)
		new_max_parsed_pos = pos;

	if (new_max_parsed_pos > max_parsed_pos)
		max_parsed_pos = new_max_parsed_pos;

	if (new_max_parsed_pos >= max_parsed_pos)
		expected = new_expected;

	if (pos > end)
		throw Exception("Logical error: pos > end.", ErrorCodes::LOGICAL_ERROR);

	if (!res)
	{
		node = nullptr;
		pos = begin;
	}

	return res;
}

}
