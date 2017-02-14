#pragma once


#include <Poco/PatternFormatter.h>
#include <Poco/Ext/ThreadNumber.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>
#include <daemon/BaseDaemon.h>

#include <experimental/optional>
#include <functional>


/** Форматирует по своему.
  * Некоторые детали невозможно получить, используя только Poco::PatternFormatter.
  *
  * Во-первых, используется номер потока не среди потоков Poco::Thread,
  *  а среди всех потоков, для которых был получен номер (см. ThreadNumber.h)
  *
  * Во-вторых, корректно выводится локальная дата и время.
  * Poco::PatternFormatter плохо работает с локальным временем,
  *  в ситуациях, когда в ближайшем будущем намечается отмена или введение daylight saving time.
  *  - см. исходники Poco и http://thread.gmane.org/gmane.comp.time.tz/8883
  *
  * Также сделан чуть более эффективным (что имеет мало значения).
  */
class OwnPatternFormatter : public Poco::PatternFormatter
{
public:
	enum Options
	{
		ADD_NOTHING = 0,
		ADD_LAYER_TAG = 1 << 0
	};

	OwnPatternFormatter(const BaseDaemon * daemon_, Options options_ = ADD_NOTHING) : Poco::PatternFormatter(""), daemon(daemon_), options(options_) {}

	void format(const Poco::Message & msg, std::string & text) override
	{
		DB::WriteBufferFromString wb(text);

		/// For syslog: tag must be before message and first whitespace.
		if (options & ADD_LAYER_TAG && daemon)
		{
			auto layer = daemon->getLayer();
			if (layer)
			{
				writeCString("layer[", wb);
				DB::writeIntText(*layer, wb);
				writeCString("]: ", wb);
			}
		}

		/// Output time with microsecond resolution.
		timeval tv;
		if (0 != gettimeofday(&tv, nullptr))
			DB::throwFromErrno("Cannot gettimeofday");

		/// Change delimiters in date for compatibility with old logs.
		DB::writeDateTimeText<'.', ':'>(tv.tv_sec, wb);

		DB::writeChar('.', wb);
		DB::writeChar('0' + ((tv.tv_usec / 100000) % 10), wb);
		DB::writeChar('0' + ((tv.tv_usec / 10000) % 10), wb);
		DB::writeChar('0' + ((tv.tv_usec / 1000) % 10), wb);
		DB::writeChar('0' + ((tv.tv_usec / 100) % 10), wb);
		DB::writeChar('0' + ((tv.tv_usec / 10) % 10), wb);
		DB::writeChar('0' + ((tv.tv_usec / 1) % 10), wb);

		writeCString(" [ ", wb);
		DB::writeIntText(Poco::ThreadNumber::get(), wb);
		writeCString(" ] <", wb);
		DB::writeString(getPriorityName(static_cast<int>(msg.getPriority())), wb);
		writeCString("> ", wb);
		DB::writeString(msg.getSource(), wb);
		writeCString(": ", wb);
		DB::writeString(msg.getText(), wb);
	}

private:
	const BaseDaemon * daemon;
	Options options;
};
