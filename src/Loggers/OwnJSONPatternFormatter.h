#pragma once


#include <Poco/PatternFormatter.h>
#include "ExtendedLogChannel.h"
#include "OwnPatternFormatter.h"


/** Format log messages own way in JSON.
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

class OwnJSONPatternFormatter : public OwnPatternFormatter
{
public:
    OwnJSONPatternFormatter();

    void format(const Poco::Message & msg, std::string & text) override;
    void formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text) const override;
};
