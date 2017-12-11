#include <Columns/Collator.h>

#include <Common/config.h>

#if USE_ICU
    #pragma GCC diagnostic push
    #ifdef __APPLE__
    #pragma GCC diagnostic ignored "-Wold-style-cast"
    #endif
    #include <unicode/ucol.h>
    #pragma GCC diagnostic pop
#else
    #if __clang__
        #pragma clang diagnostic push
        #pragma clang diagnostic ignored "-Wunused-private-field"
    #endif
#endif

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_COLLATION_LOCALE;
        extern const int COLLATION_COMPARISON_FAILED;
        extern const int SUPPORT_IS_DISABLED;
    }
}


Collator::Collator(const std::string & locale_) : locale(Poco::toLower(locale_))
{
#if USE_ICU
    UErrorCode status = U_ZERO_ERROR;

    collator = ucol_open(locale.c_str(), &status);
    if (status != U_ZERO_ERROR)
    {
        ucol_close(collator);
        throw DB::Exception("Unsupported collation locale: " + locale, DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE);
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

    if (status != U_ZERO_ERROR)
        throw DB::Exception("ICU collation comparison failed with error code: " + DB::toString<int>(status),
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
