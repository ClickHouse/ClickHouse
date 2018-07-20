#include <Formats/FormatFactory.h>
#include <Storages/System/StorageSystemFormats.h>

namespace DB
{
void StorageSystemFormats::fillData(MutableColumns & res_columns) const
{
    const auto & formats = FormatFactory::instance().getAllFormats();
    for (const auto & pair : formats)
    {
        const auto & [name, creator_pair] = pair;
        bool has_input_format = (creator_pair.first != nullptr);
        bool has_output_format = (creator_pair.second != nullptr);
        res_columns[0]->insert(name);
        std::string format_type;
        if (has_input_format)
            format_type = "input";

        if (has_output_format)
        {
            if (!format_type.empty())
                format_type += "/output";
            else
                format_type = "output";
        }

        res_columns[1]->insert(format_type);
    }
}
}
