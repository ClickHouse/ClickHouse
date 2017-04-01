#include <sstream>
#include <Core/SortDescription.h>
#include <Common/Collator.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


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
