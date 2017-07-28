#pragma once

#include <string>
#include <vector>
#include <Poco/Exception.h>
#include <common/Types.h>
#include <common/StringRef.h>


/** A class that allows you to recognize by region id its text name in one of the supported languages: ru, en, ua, by, kz, tr.
  *
  * Information about region names loads from text files with the following format names:
  *     regions_names_xx.txt,
  * where xx is one of the two letters of the following supported languages:
  *     ru, en, ua, by, kz, tr.
  *
  * Can on request update the data.
  */
class RegionsNames
{
public:
    enum class Language
    {
        RU = 0,
        EN,
        UA,
        BY,
        KZ,
        TR,
    };

private:
    static const size_t ROOT_LANGUAGE = 0;
    static const size_t SUPPORTED_LANGUAGES_COUNT = 6;
    static const size_t LANGUAGE_ALIASES_COUNT = 7;

    static const char ** getSupportedLanguages()
    {
        static const char * res[] { "ru", "en", "ua", "by", "kz", "tr" };
        return res;
    }

    struct language_alias { const char * const name; const Language lang; };
    static const language_alias * getLanguageAliases()
    {
        static constexpr const language_alias language_aliases[] {
            { "ru", Language::RU },
            { "en", Language::EN },
            { "ua", Language::UA },
            { "uk", Language::UA },
            { "by", Language::BY },
            { "kz", Language::KZ },
            { "tr", Language::TR }
        };

        return language_aliases;
    }

    using RegionID = UInt32;

    using Chars = std::vector<char>;
    using CharsForLanguageID = std::vector<Chars>;
    using ModificationTimes = std::vector<time_t>;
    using StringRefs = std::vector<StringRef>; /// Lookup table RegionID -> StringRef
    using StringRefsForLanguageID = std::vector<StringRefs>;

public:
    /** Reboot, if necessary, the names of regions.
      */
    void reload();
    void reload(const std::string & directory);

    /// Has corresponding section in configuration file.
    static bool isConfigured();


    StringRef getRegionName(RegionID region_id, Language language = Language::RU) const
    {
        size_t language_id = static_cast<size_t>(language);

        if (region_id > names_refs[language_id].size())
            return StringRef("", 0);

        StringRef ref = names_refs[language_id][region_id];

        while (ref.size == 0 && language_id != ROOT_LANGUAGE)
        {
            static const size_t FALLBACK[] = { 0, 0, 0, 0, 0, 1 };
            language_id = FALLBACK[language_id];
            ref = names_refs[language_id][region_id];
        }

        return ref;
    }

    static Language getLanguageEnum(const std::string & language)
    {
        if (language.size() == 2)
        {
            for (size_t i = 0; i < LANGUAGE_ALIASES_COUNT; ++i)
            {
                const auto & alias = getLanguageAliases()[i];
                if (language[0] == alias.name[0] && language[1] == alias.name[1])
                    return alias.lang;
            }
        }
        throw Poco::Exception("Unsupported language for region name. Supported languages are: " + dumpSupportedLanguagesNames() + ".");
    }

private:
    static std::string dumpSupportedLanguagesNames();

    ModificationTimes file_modification_times = ModificationTimes(SUPPORTED_LANGUAGES_COUNT);

    /// Bytes of names for each language, laid out in a row, separated by zeros
    CharsForLanguageID chars = CharsForLanguageID(SUPPORTED_LANGUAGES_COUNT);

    /// Mapping for each language from the region id into a pointer to the byte range of the name
    StringRefsForLanguageID names_refs = StringRefsForLanguageID(SUPPORTED_LANGUAGES_COUNT);
};
