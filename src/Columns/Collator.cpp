#include <Columns/Collator.h>

#include "config.h"

#if USE_ICU
#    include <unicode/locid.h>
#    include <unicode/ucnv.h>
#    include <unicode/ucol.h>
#    include <unicode/unistr.h>
#else
#    pragma clang diagnostic ignored "-Wunused-private-field"
#    pragma clang diagnostic ignored "-Wmissing-noreturn"
#endif

#include <Common/Exception.h>
#include <Poco/String.h>
#include <base/sort.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_COLLATION_LOCALE;
    extern const int COLLATION_COMPARISON_FAILED;
    extern const int SUPPORT_IS_DISABLED;
}
}


AvailableCollationLocales::AvailableCollationLocales()
{
#if USE_ICU
    static const size_t MAX_LANG_LENGTH = 128;
    int32_t available_locales_count = ucol_countAvailable();
    for (int32_t i = 0; i < available_locales_count; ++i)
    {
        std::string locale_name = ucol_getAvailable(i);
        UChar lang_buffer[MAX_LANG_LENGTH];
        char normal_buf[MAX_LANG_LENGTH];
        UErrorCode status = U_ZERO_ERROR;

        /// All names will be in English language
        int32_t lang_length = uloc_getDisplayLanguage(
            locale_name.c_str(), "en", lang_buffer, MAX_LANG_LENGTH, &status);
        std::optional<std::string> lang;

        if (!U_FAILURE(status))
        {
            /// Convert language name from UChar array to normal char array.
            /// We use English language for name, so all UChar's length is equal to sizeof(char)
            u_UCharsToChars(lang_buffer, normal_buf, lang_length);
            lang.emplace(std::string(normal_buf, lang_length));
        }

        locales_map.emplace(Poco::toLower(locale_name), LocaleAndLanguage{locale_name, lang});
    }

#endif
}

const AvailableCollationLocales & AvailableCollationLocales::instance()
{
    static AvailableCollationLocales instance;
    return instance;
}

AvailableCollationLocales::LocalesVector AvailableCollationLocales::getAvailableCollations() const
{
    LocalesVector result;
    for (const auto & name_and_locale : locales_map)
        result.push_back(name_and_locale.second);

    auto comparator = [] (const LocaleAndLanguage & f, const LocaleAndLanguage & s)
    {
        return f.locale_name < s.locale_name;
    };
    ::sort(result.begin(), result.end(), comparator);

    return result;
}

bool AvailableCollationLocales::isCollationSupported(const std::string & locale_name) const
{
    /// We support locale names in any case, so we have to convert all to lower case
    return locales_map.contains(Poco::toLower(locale_name));
}

Collator::Collator(const std::string & locale_)
    : locale(Poco::toLower(locale_))
{
#if USE_ICU
    /// ICU locales can have settings and keywords, e.g. 'tr-u-kn-true-ka-shifted' is 'Turkish' with keywords.
    /// See https://peter.eisentraut.org/blog/2023/05/16/overview-of-icu-collation-settings for details.
    /// Remove these as AvailableCollationLocales only knows the base names.
    static const size_t MAX_BASE_NAME_LENGTH = 128;
    char base_name_buf[MAX_BASE_NAME_LENGTH];
    UErrorCode status = U_ZERO_ERROR;
    size_t base_name_length = uloc_getBaseName(locale.c_str(), base_name_buf, MAX_BASE_NAME_LENGTH, &status);
    if (U_FAILURE(status))
        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE, "Failed to get base name for locale: {}. Error: {}", locale, u_errorName(status));
    std::string base_locale = {base_name_buf, base_name_length};

    /// We check it here, because ucol_open will fallback to default locale for
    /// almost all random names.
    if (!AvailableCollationLocales::instance().isCollationSupported(base_locale))
        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE, "Unsupported collation locale: {}", locale);

    collator = ucol_open(locale.c_str(), &status);
    if (U_FAILURE(status))
    {
        ucol_close(collator);
        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE, "Failed to open locale: {} with error: {}", locale, u_errorName(status));
    }
#else
    throw DB::Exception(DB::ErrorCodes::SUPPORT_IS_DISABLED,
                        "Collations support is disabled, because ClickHouse was built without ICU library");
#endif
}


Collator::~Collator() // NOLINT
{
#if USE_ICU
    ucol_close(collator);
#endif
}

int Collator::compare(const char * str1, size_t length1, const char * str2, size_t length2) const
{
#if USE_ICU
    UCharIterator iter1;
    UCharIterator iter2;
    uiter_setUTF8(&iter1, str1, static_cast<int32_t>(length1));
    uiter_setUTF8(&iter2, str2, static_cast<int32_t>(length2));

    UErrorCode status = U_ZERO_ERROR;
    UCollationResult compare_result = ucol_strcollIter(collator, &iter1, &iter2, &status);

    if (U_FAILURE(status))
        throw DB::Exception(DB::ErrorCodes::COLLATION_COMPARISON_FAILED, "ICU collation comparison failed with error code: {}",
                            std::string(u_errorName(status)));

    /** Values of enum UCollationResult are equals to what exactly we need:
     *     UCOL_EQUAL = 0
     *     UCOL_GREATER = 1
     *     UCOL_LESS = -1
     */
    return compare_result;
#else
    (void)str1;
    (void)length1;
    (void)str2;
    (void)length2;
    return 0;
#endif
}

const std::string & Collator::getLocale() const
{
    return locale;
}
