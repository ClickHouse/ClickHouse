#include <Dictionaries/Embedded/GeodataProviders/NamesFormatReader.h>

#include <IO/ReadHelpers.h>


bool LanguageRegionsNamesFormatReader::readNext(RegionNameEntry & entry)
{
    while (!input->eof())
    {
        Int32 read_region_id;
        std::string region_name;

        DB::readIntText(read_region_id, *input);
        DB::assertChar('\t', *input);
        DB::readString(region_name, *input);
        DB::assertChar('\n', *input);

        if (read_region_id <= 0)
            continue;

        entry.id = read_region_id;
        entry.name = region_name;
        return true;
    }

    return false;
}
