#include <Core/SortDescription.h>
#include <Core/Block.h>

namespace DB
{

String dumpSortDescription(const SortDescription & description, const Block & header)
{
    String res;

    for (const auto & desc : description)
    {
        if (!res.empty())
            res += ", ";

        if (!desc.column_name.empty())
            res += desc.column_name;
        else
        {
            if (desc.column_number < header.columns())
                res += header.getByPosition(desc.column_number).name;
            else
                res += "?";

            res += " (pos " + std::to_string(desc.column_number) + ")";
        }

        if (desc.direction > 0)
            res += " ASC";
        else
            res += " DESC";

        if (desc.with_fill)
            res += " WITH FILL";
    }

    return res;
}

}

