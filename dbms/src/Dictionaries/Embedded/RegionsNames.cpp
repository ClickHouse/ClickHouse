#include <Dictionaries/Embedded/RegionsNames.h>

#include <Poco/File.h>
#include <Poco/Util/Application.h>
#include <Poco/Exception.h>

#include <common/logger_useful.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFile.h>


static constexpr auto config_key = "path_to_regions_names_files";


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
    std::string directory = Poco::Util::Application::instance().config().getString(config_key);
    reload(directory);
}

void RegionsNames::reload(const std::string & directory)
{
    Logger * log = &Logger::get("RegionsNames");
    LOG_DEBUG(log, "Reloading regions names");

    RegionID max_region_id = 0;
    for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
    {
        const std::string & language = getSupportedLanguages()[language_id];
        std::string path = directory + "/regions_names_" + language + ".txt";

        Poco::File file(path);
        time_t new_modification_time = file.getLastModified().epochTime();
        if (new_modification_time <= file_modification_times[language_id])
            continue;
        file_modification_times[language_id] = new_modification_time;

        LOG_DEBUG(log, "Reloading regions names for language: " << language);

        DB::ReadBufferFromFile in(path);

        const size_t initial_size = 10000;
        const size_t max_size = 15000000;

        Chars new_chars;
        StringRefs new_names_refs(initial_size, StringRef("", 0));

        /// Allocate a continuous slice of memory, which is enough to store all names.
        new_chars.reserve(Poco::File(path).getSize());

        while (!in.eof())
        {
            Int32 read_region_id;
            std::string region_name;

            DB::readIntText(read_region_id, in);
            DB::assertChar('\t', in);
            DB::readString(region_name, in);
            DB::assertChar('\n', in);

            if (read_region_id <= 0)
                continue;

            RegionID region_id = read_region_id;

            size_t old_size = new_chars.size();

            if (new_chars.capacity() < old_size + region_name.length() + 1)
                throw Poco::Exception("Logical error. Maybe size of file " + path + " is wrong.");

            new_chars.resize(old_size + region_name.length() + 1);
            memcpy(&new_chars[old_size], region_name.c_str(), region_name.length() + 1);

            if (region_id > max_region_id)
            {
                max_region_id = region_id;

                if (region_id > max_size)
                    throw DB::Exception("Region id is too large: " + DB::toString(region_id) + ", should be not more than " + DB::toString(max_size));
            }

            while (region_id >= new_names_refs.size())
                new_names_refs.resize(new_names_refs.size() * 2, StringRef("", 0));

            new_names_refs[region_id] = StringRef(&new_chars[old_size], region_name.length());
        }

        chars[language_id].swap(new_chars);
        names_refs[language_id].swap(new_names_refs);
    }

    for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
        names_refs[language_id].resize(max_region_id + 1, StringRef("", 0));
}


bool RegionsNames::isConfigured()
{
    return Poco::Util::Application::instance().config().has(config_key);
}
