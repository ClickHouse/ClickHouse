#pragma once

#include <string>
#include <boost/noncopyable.hpp>

struct UCollator;

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
	explicit Collator(const std::string & locale_);
	~Collator();

	int compare(const char * str1, size_t length1, const char * str2, size_t length2) const;

	const std::string & getLocale() const;

private:
	std::string locale;
	UCollator * collator;
};
