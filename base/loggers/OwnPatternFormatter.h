#pragma once


#include <Poco/PatternFormatter.h>
#include "ExtendedLogChannel.h"


/** Format log messages own way.
  * We can't obtain some details using Poco::PatternFormatter.
  *
  * Firstly, the thread number here is peaked not from Poco::Thread
  * threads only, but from all threads with number assigned (see ThreadNumber.h)
  *
  * Secondly, the local date and time are correctly displayed.
  * Poco::PatternFormatter does not work well with local time,
  * when timestamps are close to DST timeshift moments.
  * - see Poco sources and http://thread.gmane.org/gmane.comp.time.tz/8883
  *
  * Also it's made a bit more efficient (unimportant).
  */

class Loggers;

class OwnPatternFormatter : public Poco::PatternFormatter
{
public:
    /// ADD_LAYER_TAG is needed only for Yandex.Metrika, that share part of ClickHouse code.
    enum Options
    {
        ADD_NOTHING = 0,
        ADD_LAYER_TAG = 1 << 0
    };

    OwnPatternFormatter(const Loggers * loggers_, Options options_ = ADD_NOTHING, bool color_ = false);

    void format(const Poco::Message & msg, std::string & text) override;
    void formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text);

private:
    const Loggers * loggers;
    Options options;
    bool color;
};
