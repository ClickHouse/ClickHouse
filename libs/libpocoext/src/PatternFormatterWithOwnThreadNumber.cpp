#include <Poco/Ext/ThreadNumber.h>
#include <Poco/Ext/PatternFormatterWithOwnThreadNumber.h>


namespace Poco {

void PatternFormatterWithOwnThreadNumber::format(const Message & msg, std::string & text)
{
	Poco::Message tmp_message(msg);
	tmp_message.setTid(ThreadNumber::get());
	PatternFormatter::format(tmp_message, text);
}

}
