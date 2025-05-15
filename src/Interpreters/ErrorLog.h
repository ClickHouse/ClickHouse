#pragma once

#include <Interpreters/PeriodicLog.h>
#include <Common/ErrorCodes.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/ColumnsDescription.h>

#include <vector>


namespace DB
{

/** ErrorLog is a log of error values measured at regular time interval.
  */

struct ErrorLogElement
{
    time_t event_time{};
    ErrorCodes::ErrorCode code{};
    UInt64 error_time_ms = 0;
    std::string error_message{};
    ErrorCodes::Value value{};
    bool remote{};
    std::string query_id;
    std::vector<void *> error_trace{};
    bool symbolize = false;

    static std::string name() { return "ErrorLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class ErrorLog : public PeriodicLog<ErrorLogElement>
{
    using PeriodicLog<ErrorLogElement>::PeriodicLog;

public:
    ErrorLog(ContextPtr context_,
             const SystemLogSettings & settings_,
             std::shared_ptr<SystemLogQueue<ErrorLogElement>> queue_ = nullptr)
        : PeriodicLog<ErrorLogElement>(context_, settings_, queue_),
        symbolize(settings_.symbolize_traces)
    {
    }

protected:
    void stepFunction(TimePoint current_time) override;

private:
    bool symbolize;
};

}
