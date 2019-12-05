#include <Columns/Collator.h>

#include "config_core.h"

#if USE_ICU
    #include <unicode/ucol.h>
    #include <unicode/unistr.h>
    #include <unicode/locid.h>
    #include <unicode/ucnv.h>
#else
    #ifdef __clang__
        #pragma clang diagnostic ignored "-Wunused-private-field"
        #pragma clang diagnostic ignored "-Wmissing-noreturn"
    #endif
#endif

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <algorithm>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_COLLATION_LOCALE;
        extern const int COLLATION_COMPARISON_FAILED;
        extern const int SUPPORT_IS_DISABLED;
    }
}


std::unique_ptr<AvailableCollationLocales> AvailableCollationLocales::instance_impl;
std::once_flag AvailableCollationLocales::init_flag;

void AvailableCollationLocales::init()
{
    instance_impl = std::make_unique<AvailableCollationLocales>();
#if USE_ICU
    size_t available_locales_count = ucol_countAvailable();
    for (size_t i = 0; i < available_locales_count; ++i)
    {
        std::string locale_name = ucol_getAvailable(i);
        UChar lang_buffer[128]; /// 128 is enough for language name
        char normal_buf[128];
        UErrorCode status = U_ZERO_ERROR;
        /// All names will be in English language
        size_t lang_length = uloc_getDisplayLanguage(locale_name.c_str(), "en", lang_buffer, 128, &status);
        if (U_FAILURE(status))
            instance_impl->available_collation_locales.push_back(LocaleAndLanguage{locale_name, "unknown"});
        else
        {
            u_UCharsToChars(lang_buffer, normal_buf, lang_length);
            LocaleAndLanguage result{locale_name, std::string(normal_buf, lang_length)};
            instance_impl->available_collation_locales.push_back(result);
        }
    }

    auto comparator = [] (const LocaleAndLanguage & f, const LocaleAndLanguage & s) { return f.locale_name < s.locale_name; };
    std::sort(instance_impl->available_collation_locales.begin(), instance_impl->available_collation_locales.end(), comparator);
#endif
}

AvailableCollationLocales & AvailableCollationLocales::instance()
{
    std::call_once(init_flag, AvailableCollationLocales::init);
    return *instance_impl;
}

const std::vector<AvailableCollationLocales::LocaleAndLanguage> & AvailableCollationLocales::getAvailableCollations() const
{
    return available_collation_locales;
}

bool AvailableCollationLocales::isCollationSupported(const std::string & s) const
{
    std::string lower = Poco::toLower(s);
    for (const auto & locale_and_lang : available_collation_locales)
    {
        if (lower == Poco::toLower(locale_and_lang.locale_name))
            return true;
    }
    return false;
}

Collator::Collator(const std::string & locale_) : locale(Poco::toLower(locale_))
{
#if USE_ICU
    /// We check it here, because ucol_open will fallback to default locale for
    /// almost all random names.
    if (!AvailableCollationLocales::instance().isCollationSupported(locale))
        throw DB::Exception("Unsupported collation locale: " + locale, DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE);

    UErrorCode status = U_ZERO_ERROR;

    collator = ucol_open(locale.c_str(), &status);
    if (U_FAILURE(status))
    {
        ucol_close(collator);
        throw DB::Exception("Failed to open locale: " + locale + " with error: " + u_errorName(status), DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE);
    }
#else
    throw DB::Exception("Collations support is disabled, because ClickHouse was built without ICU library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}


Collator::~Collator()
{
#if USE_ICU
    ucol_close(collator);
#endif
}

int Collator::compare(const char * str1, size_t length1, const char * str2, size_t length2) const
{
#if USE_ICU
    UCharIterator iter1, iter2;
    uiter_setUTF8(&iter1, str1, length1);
    uiter_setUTF8(&iter2, str2, length2);

    UErrorCode status = U_ZERO_ERROR;
    UCollationResult compare_result = ucol_strcollIter(collator, &iter1, &iter2, &status);

    if (U_FAILURE(status))
        throw DB::Exception("ICU collation comparison failed with error code: " + std::string(u_errorName(status)),
                            DB::ErrorCodes::COLLATION_COMPARISON_FAILED);

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
