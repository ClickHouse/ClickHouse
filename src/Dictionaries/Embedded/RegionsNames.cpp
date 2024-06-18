#include "RegionsNames.h"

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Poco/Exception.h>
#include <Common/logger_useful.h>
#include "GeodataProviders/INamesProvider.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

RegionsNames::RegionsNames(IRegionsNamesDataProviderPtr data_provider)
{
    for (size_t language_id = 0; language_id < total_languages; ++language_id)
    {
        const std::string & language = languages[language_id];
        names_sources[language_id] = data_provider->getLanguageRegionsNamesSource(language);
    }

    reload();
}

std::string RegionsNames::dumpSupportedLanguagesNames()
{
    WriteBufferFromOwnString out;
    for (size_t i = 0; i < total_languages; ++i)
    {
        if (i > 0)
            out << ", ";
        out << '\'' << languages[i] << '\'';
    }
    return out.str();
}

void RegionsNames::reload()
{
    LoggerPtr log = getLogger("RegionsNames");
    LOG_DEBUG(log, "Reloading regions names");

    RegionID max_region_id = 0;
    for (size_t language_id = 0; language_id < total_languages; ++language_id)
    {
        const std::string & language = languages[language_id];

        auto names_source = names_sources[language_id];

        if (!names_source || !names_source->isModified())
            continue;

        LOG_DEBUG(log, "Reloading regions names for language: {}", language);

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
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Logical error. Maybe size estimate of {} is wrong", names_source->getSourceName());

            new_chars.resize(old_size + name_entry.name.length() + 1);
            memcpy(new_chars.data() + old_size, name_entry.name.c_str(), name_entry.name.length() + 1);

            if (name_entry.id > max_region_id)
            {
                max_region_id = name_entry.id;

                if (name_entry.id > max_size)
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA, "Region id is too large: {}, should be not more than {}", name_entry.id, max_size);
            }

            while (name_entry.id >= new_names_refs.size())
                new_names_refs.resize(new_names_refs.size() * 2, StringRef("", 0));

            new_names_refs[name_entry.id] = StringRef(new_chars.data() + old_size, name_entry.name.length());
        }

        chars[language_id].swap(new_chars);
        names_refs[language_id].swap(new_names_refs);
    }

    for (size_t language_id = 0; language_id < total_languages; ++language_id)
        names_refs[language_id].resize(max_region_id + 1, StringRef("", 0));
}

}
