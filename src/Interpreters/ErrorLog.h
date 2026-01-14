#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ErrorCodes.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/** ErrorLog is a log of error values measured at regular time interval.
  */

struct ErrorLogElement
{
    time_t event_time{};
    ErrorCodes::ErrorCode code{};
    ErrorCodes::Value value{};
    bool remote{};

    static std::string name() { return "ErrorLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class ErrorLog : public PeriodicLog<ErrorLogElement>
{
    using PeriodicLog<ErrorLogElement>::PeriodicLog;

protected:
    void stepFunction(TimePoint current_time) override;

private:
    struct ValuePair
    {
        UInt64 local = 0;
        UInt64 remote = 0;
    };
    /// stepFunction and flushBufferToLog may be executed concurrently, hence the mutex
    std::vector<ValuePair> previous_values TSA_GUARDED_BY(previous_values_mutex) = std::vector<ValuePair>(ErrorCodes::end());
    mutable std::mutex previous_values_mutex;
};

}
