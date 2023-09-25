#include "ContextExpireChecker.h"

#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include "WriteBufferFromString.h"

//#include <Interpreters/Context.h>

namespace DB
{

ContextExpireChecker::~ContextExpireChecker()
{
    Status at_end;

    bool interesting_case = false;
    String explanation;

    if (atStart.hasThreadStatus and !at_end.hasThreadStatus)
    {
        interesting_case = true;
        std::string_view reason = " thread status gone;";
        explanation.append(reason);
    }

    if (atStart.hasAttachedThreadGroup and !at_end.hasAttachedThreadGroup)
    {
        interesting_case = true;
        std::string_view reason = " thread group gone;";
        explanation.append(reason);
    }

    if (atStart.hasQueryID and !at_end.hasQueryID)
    {
        interesting_case = true;
        std::string_view reason = " query id gone;";
        explanation.append(reason);
    }

    if (atStart.hasValidQueryContext and !at_end.hasValidQueryContext)
    {
        interesting_case = true;
        std::string_view reason = " query context gone;";
        explanation.append(reason);
    }

    if (atStart.hasProcessListElem and !at_end.hasProcessListElem)
    {
        interesting_case = true;
        std::string_view reason = " process list element gone;";
        explanation.append(reason);
    }

    if (interesting_case)
    {
        Poco::Logger * log = &Poco::Logger::get("ContextExpireChecker");
        LOG_ERROR(
            log,
            "anomaly caught, explain: {}, created at: {}, destroyed at: {}",
            explanation,
            atStart.stack.toString(),
            at_end.stack.toString());
        chassert(false && "WriteBuffer is not finalized in destructor.");
    }
}

ContextExpireChecker::Status::Status()
{
    hasThreadStatus = CurrentThread::isInitialized();
    hasAttachedThreadGroup = bool(CurrentThread::getGroup());
    hasQueryID = !CurrentThread::getQueryId().empty();

//    if (auto context = CurrentThread::getQueryContext())
//    {
//        hasValidQueryContext = true;
//        hasProcessListElem = bool(context->getProcessListElementSafe());
//    }
}

}
