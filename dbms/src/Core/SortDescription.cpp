#include <DB/Core/SortDescription.h>
#include <DB/Common/Collator.h>

namespace DB
{

String SortColumnDescription::getID() const
{
	std::stringstream res;
	res << column_name << ", " << column_number << ", " << direction;
	if (collator)
		res << ", collation locale: " << collator->getLocale();
	return res.str();
}

}
