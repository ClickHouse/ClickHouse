#include <sstream>
#include <DB/Core/SortDescription.h>
#include <DB/Common/Collator.h>
#include <DB/IO/Operators.h>
#include <DB/IO/WriteBufferFromString.h>


namespace DB
{

std::string SortColumnDescription::getID() const
{
	std::string res;
	{
		WriteBufferFromString out(res);
		out << column_name << ", " << column_number << ", " << direction << ", " << nulls_direction;
		if (collator)
			out << ", collation locale: " << collator->getLocale();
	}
	return res;
}

}
