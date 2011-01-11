#ifndef Ext_Foundation_PatternFormatter_INCLUDED
#define Ext_Foundation_PatternFormatter_INCLUDED

#include <Poco/PatternFormatter.h>


namespace Poco {

/** Отличается от PatternFormatter тем, что использует номер потока не среди
  * потоков Poco::Thread, а среди всех потоков, для которых был получен номер (см. ThreadNumber.h)
  */
class Foundation_API PatternFormatterWithOwnThreadNumber : public PatternFormatter
{
public:
	PatternFormatterWithOwnThreadNumber() {}
	PatternFormatterWithOwnThreadNumber(const std::string & format) : PatternFormatter(format) {}

	void format(const Message & msg, std::string & text);
};

}

#endif
