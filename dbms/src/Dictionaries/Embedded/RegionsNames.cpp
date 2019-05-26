#include "RegionsNames.h"

#include <IO/WriteHelpers.h>
#include <Poco/Exception.h>
#include <Poco/Util/Application.h>
#include <common/logger_useful.h>
#include "GeodataProviders/INamesProvider.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}


RegionsNames::RegionsNames(IRegionsNamesDataProviderPtr data_provider)
{
    for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
    {
        const std::string & language = getSupportedLanguages()[language_id];
        names_sources[language_id] = data_provider->getLanguageRegionsNamesSource(language);
    }

    reload();
}

std::string RegionsNames::dumpSupportedLanguagesNames()
{
    std::string res = "";
    for (size_t i = 0; i < LANGUAGE_ALIASES_COUNT; ++i)
    {
        if (i > 0)
            res += ", ";
        res += '\'';
        res += getLanguageAliases()[i].name;
        res += '\'';
    }
    return res;
}

void RegionsNames::reload()
{
    Logger * log = &Logger::get("RegionsNames");
    LOG_DEBUG(log, "Reloading regions names");

    RegionID max_region_id = 0;
    for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
    {
        const std::string & language = getSupportedLanguages()[language_id];

        auto names_source = names_sources[language_id];

        if (!names_source->isModified())
            continue;

        LOG_DEBUG(log, "Reloading regions names for language: " << language);

        auto names_reader = names_source->createReader();

        const size_t initial_size = 10000;
        const size_t max_size = 15000000;

        Chars new_chars;
        StringRefs new_names_refs(initial_size, StringRef("", 0));

        /// Allocate a continuous slice of memory, which is enough to store all names.
        new_chars.reserve(names_source->estimateTotalSize());

        RegionNameEntry name_entry;
        while (names_reader->readNext(name_entry))
        {
            size_t old_size = new_chars.size();

            if (new_chars.capacity() < old_size + name_entry.name.length() + 1)
                throw Poco::Exception("Logical error. Maybe size estimate of " + names_source->getSourceName() + " is wrong.");

            new_chars.resize(old_size + name_entry.name.length() + 1);
            memcpy(new_chars.data() + old_size, name_entry.name.c_str(), name_entry.name.length() + 1);

            if (name_entry.id > max_region_id)
            {
                max_region_id = name_entry.id;

                if (name_entry.id > max_size)
                    throw DB::Exception(
                        "Region id is too large: " + DB::toString(name_entry.id) + ", should be not more than " + DB::toString(max_size),
                        DB::ErrorCodes::INCORRECT_DATA);
            }

            while (name_entry.id >= new_names_refs.size())
                new_names_refs.resize(new_names_refs.size() * 2, StringRef("", 0));

            new_names_refs[name_entry.id] = StringRef(new_chars.data() + old_size, name_entry.name.length());
        }

        chars[language_id].swap(new_chars);
        names_refs[language_id].swap(new_names_refs);
    }

    for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
        names_refs[language_id].resize(max_region_id + 1, StringRef("", 0));
}
