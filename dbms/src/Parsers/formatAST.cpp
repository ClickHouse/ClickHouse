#include <DB/Parsers/formatAST.h>


namespace DB
{

String formatColumnsForCreateQuery(NamesAndTypesList & columns)
{
	std::string res;
	res += "(";
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		if (it != columns.begin())
			res += ", ";
		res += backQuoteIfNeed(it->name);
		res += " ";
		res += it->type->getName();
	}
	res += ")";
	return res;
}

}
