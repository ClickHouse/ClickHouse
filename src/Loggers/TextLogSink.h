#pragma once

#include <atomic>
#include <memory>

#ifndef WITHOUT_TEXT_LOG
namespace DB
{
    template <typename> class SystemLogQueue;
    struct TextLogElement;
    using TextLogQueue = SystemLogQueue<TextLogElement>;
}
#endif

namespace DB
{

class ExtendedLogMessage;

/// Works as Poco::SplitterChannel, but performs additional work:
///  passes logs to Client via TCP interface
///  tries to use extended logging interface of child for more comprehensive logging
class TextLogSink
{
public:
    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const ExtendedLogMessage & msg);

#ifndef WITHOUT_TEXT_LOG
    void addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority);
#endif
private:
#ifndef WITHOUT_TEXT_LOG
    std::weak_ptr<DB::TextLogQueue> text_log;
    std::atomic<int> text_log_max_priority = -1;
#endif
};

}
