#pragma once


#include <Poco/PatternFormatter.h>
#include "ExtendedLogChannel.h"


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

    OwnPatternFormatter(const Loggers * loggers_, Options options_ = ADD_NOTHING);

    void format(const Poco::Message & msg, std::string & text) override;
    void formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text);

private:
    const Loggers * loggers;
    Options options;
};
