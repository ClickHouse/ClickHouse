#pragma once

#include <string>
#include <vector>
#include <Poco/Exception.h>
#include <common/StringRef.h>
#include <common/types.h>
#include "GeodataProviders/INamesProvider.h"


/** A class that allows you to recognize by region id its text name in one of the supported languages.
  *
  * Information about region names loads from text files with the following format names: regions_names_xx.txt,
  * where xx is one of the two letters of the following supported languages.
  *
  * Can on request update the data.
  */
class RegionsNames
{
/// Language name and fallback language name.
#define FOR_EACH_LANGUAGE(M) \
    M(ru, ru, 0) \
    M(en, ru, 1) \
    M(ua, ru, 2) \
    M(uk, ua, 3) \
    M(by, ru, 4) \
    M(kz, ru, 5) \
    M(tr, en, 6) \
    M(de, en, 7) \
    M(uz, ru, 8) \
    M(lv, ru, 9) \
    M(lt, ru, 10) \
    M(et, ru, 11) \
    M(pt, en, 12) \
    M(he, en, 13) \
    M(vi, en, 14)

    static constexpr size_t total_languages = 15;

public:
    enum class Language : size_t
    {
        #define M(NAME, FALLBACK, NUM) NAME = NUM,
        FOR_EACH_LANGUAGE(M)
    #undef M
    };

private:
    static inline constexpr const char * languages[] =
    {
        #define M(NAME, FALLBACK, NUM) #NAME,
        FOR_EACH_LANGUAGE(M)
        #undef M
    };

    static inline constexpr Language fallbacks[] =
    {
        #define M(NAME, FALLBACK, NUM) Language::FALLBACK,
        FOR_EACH_LANGUAGE(M)
        #undef M
    };

    using NamesSources = std::vector<std::shared_ptr<ILanguageRegionsNamesDataSource>>;

    using Chars = std::vector<char>;
    using CharsForLanguageID = std::vector<Chars>;
    using StringRefs = std::vector<StringRef>; /// Lookup table RegionID -> StringRef
    using StringRefsForLanguageID = std::vector<StringRefs>;


    NamesSources names_sources = NamesSources(total_languages);

    /// Bytes of names for each language, laid out in a row, separated by zeros
    CharsForLanguageID chars = CharsForLanguageID(total_languages);

    /// Mapping for each language from the region id into a pointer to the byte range of the name
    StringRefsForLanguageID names_refs = StringRefsForLanguageID(total_languages);

    static std::string dumpSupportedLanguagesNames();
public:
    RegionsNames(IRegionsNamesDataProviderPtr data_provider);

    StringRef getRegionName(RegionID region_id, Language language) const
    {
        size_t language_id = static_cast<size_t>(language);

        if (region_id >= names_refs[language_id].size()) //-V1051
            return StringRef("", 0);

        StringRef ref = names_refs[language_id][region_id];

        static constexpr size_t root_language = static_cast<size_t>(Language::ru);
        while (ref.size == 0 && language_id != root_language)
        {
            language_id = static_cast<size_t>(fallbacks[language_id]);
            ref = names_refs[language_id][region_id];
        }

        return ref;
    }

    static Language getLanguageEnum(const std::string & language)
    {
        #define M(NAME, FALLBACK, NUM) \
            if (0 == language.compare(#NAME)) \
                return Language::NAME;
        FOR_EACH_LANGUAGE(M)
        #undef M
        throw Poco::Exception("Unsupported language for region name. Supported languages are: " + dumpSupportedLanguagesNames() + ".");
    }

    void reload();
};
