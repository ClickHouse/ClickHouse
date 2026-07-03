#include <Dictionaries/Embedded/GeodataProviders/NamesFormatReader.h>

#include <IO/ReadHelpers.h>

namespace DB
{

bool LanguageRegionsNamesFormatReader::readNext(RegionNameEntry & entry)
{
    while (!input->eof())
    {
        Int32 read_region_id;
        std::string region_name;

        readIntText(read_region_id, *input);
        assertChar('\t', *input);
        readString(region_name, *input);
        assertChar('\n', *input);

        if (read_region_id <= 0)
            continue;

        entry.id = read_region_id;
        entry.name = region_name;
        return true;
    }

    return false;
}

}
