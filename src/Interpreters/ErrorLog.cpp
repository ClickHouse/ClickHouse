#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ErrorLog.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/ThreadPool.h>
#include <Common/ErrorCodes.h>

#include <vector>

namespace DB
{

ColumnsDescription ErrorLogElement::getColumnsDescription()
{
    return ColumnsDescription {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"code", std::make_shared<DataTypeInt32>(), "Error code}"},
        {"error", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Error name."},
        {"value", std::make_shared<DataTypeUInt64>(), "Number of errors happened in time interval."},
        {"remote", std::make_shared<DataTypeUInt8>(), "Remote exception (i.e. received during one of the distributed queries)."}
    };
}

void ErrorLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(code);
    columns[column_idx++]->insert(ErrorCodes::getName(code));
    columns[column_idx++]->insert(value);
    columns[column_idx++]->insert(remote);
}

void ErrorLog::startCollectError(size_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;
    is_shutdown_error_thread = false;
    flush_thread = std::make_unique<ThreadFromGlobalPool>([this] { threadFunction(); });
}


void ErrorLog::stopCollect()
{
    bool old_val = false;
    if (!is_shutdown_error_thread.compare_exchange_strong(old_val, true))
        return;
    if (flush_thread)
        flush_thread->join();
}


void ErrorLog::shutdown()
{
    stopCollect();
    stopFlushThread();
}

void ErrorLog::threadFunction()
{
    auto desired_timepoint = std::chrono::system_clock::now();
    std::vector<PreviousValue> previous_values(ErrorCodes::end());

    while (!is_shutdown_error_thread)
    {
        try
        {
            const auto current_time = std::chrono::system_clock::now();
            auto event_time = std::chrono::system_clock::to_time_t(current_time);

            for (ErrorCodes::ErrorCode code = 0, end = ErrorCodes::end(); code < end; ++code)
            {
                const auto & error = ErrorCodes::values[code].get();
                if (error.local.count != previous_values.at(code).local)
                {
                    ErrorLogElement local_elem {
                        .event_time=event_time,
                        .code=code,
                        .value=error.local.count - previous_values.at(code).local,
                        .remote=false
                    };
                    this->add(std::move(local_elem));
                    previous_values[code].local = error.local.count;
                }
                if (error.remote.count != previous_values.at(code).remote)
                {
                    ErrorLogElement remote_elem {
                        .event_time=event_time,
                        .code=code,
                        .value=error.remote.count - previous_values.at(code).remote,
                        .remote=true
                    };
                    this->add(std::move(remote_elem));
                    previous_values[code].remote = error.remote.count;
                }
            }

            /// We will record current time into table but align it to regular time intervals to avoid time drift.
            /// We may drop some time points if the server is overloaded and recording took too much time.
            while (desired_timepoint <= current_time)
                desired_timepoint += std::chrono::milliseconds(collect_interval_milliseconds);

            std::this_thread::sleep_until(desired_timepoint);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
