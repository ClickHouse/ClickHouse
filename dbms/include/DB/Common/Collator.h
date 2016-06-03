#pragma once

#include <unicode/ucol.h>

#include <DB/Common/Exception.h>
#include <DB/IO/WriteHelpers.h>

#include <common/Common.h>

#include <Poco/String.h>

#include <boost/noncopyable.hpp>


namespace DB
{
	namespace ErrorCodes
	{
		extern const int UNSUPPORTED_COLLATION_LOCALE;
		extern const int COLLATION_COMPARISON_FAILED;
	}
}


class Collator : private boost::noncopyable
{
public:
	explicit Collator(const std::string & locale_) : locale(Poco::toLower(locale_))
	{		
		UErrorCode status = U_ZERO_ERROR;
		
		collator = ucol_open(locale.c_str(), &status);
		if (status != U_ZERO_ERROR)
		{
			ucol_close(collator);
			throw DB::Exception("Unsupported collation locale: " + locale, DB::ErrorCodes::UNSUPPORTED_COLLATION_LOCALE);
		}
	}
	
	~Collator()
	{
		ucol_close(collator);
	}
	
	int compare(const char * str1, size_t length1, const char * str2, size_t length2) const
	{
		UCharIterator iter1, iter2;
		uiter_setUTF8(&iter1, str1, length1);
		uiter_setUTF8(&iter2, str2, length2);
		
		UErrorCode status = U_ZERO_ERROR;
		UCollationResult compare_result = ucol_strcollIter(collator, &iter1, &iter2, &status);
		
		if (status != U_ZERO_ERROR)
			throw DB::Exception("ICU collation comparison failed with error code: " + DB::toString(status),
								DB::ErrorCodes::COLLATION_COMPARISON_FAILED);
		
		/** Значения enum UCollationResult совпадают с нужными нам:
		 * 	UCOL_EQUAL = 0
		 * 	UCOL_GREATER = 1
		 * 	UCOL_LESS = -1
		 */
		return compare_result;
	}
	
	const std::string & getLocale() const
	{
		return locale;
	}
	
private:
	std::string locale;
	UCollator * collator;
};
