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
    WriteBufferFromOwnString explanation;

    if (atStart.hasThreadStatus and !at_end.hasThreadStatus)
    {
        interesting_case = true;
        std::string_view reason = " thread status gone;";
        explanation.write(reason.data(), reason.size());
    }

    if (atStart.hasAttachedThreadGroup and !at_end.hasAttachedThreadGroup)
    {
        interesting_case = true;
        std::string_view reason = " thread group gone;";
        explanation.write(reason.data(), reason.size());
    }

    if (atStart.hasQueryID and !at_end.hasQueryID)
    {
        interesting_case = true;
        std::string_view reason = " query id gone;";
        explanation.write(reason.data(), reason.size());
    }

    if (atStart.hasValidQueryContext and !at_end.hasValidQueryContext)
    {
        interesting_case = true;
        std::string_view reason = " query context gone;";
        explanation.write(reason.data(), reason.size());
    }

    if (atStart.hasProcessListElem and !at_end.hasProcessListElem)
    {
        interesting_case = true;
        std::string_view reason = " process list element gone;";
        explanation.write(reason.data(), reason.size());
    }

    if (interesting_case)
    {
        Poco::Logger * log = &Poco::Logger::get("ContextExpireChecker");
        LOG_ERROR(
            log,
            "anomaly caught, explain: {}, created at: {}, destroyed at: {}",
            explanation.str(),
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
