#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/StorageID.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

struct BackgroundSchedulePoolLogElement
{
    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;

    String query_id;
    String database_name;
    String table_name;
    UUID table_uuid{UUIDHelpers::Nil};
    String log_name;

    UInt64 duration_ms = 0;

    UInt16 error = 0;
    String exception;

    static std::string name() { return "BackgroundSchedulePoolLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};


/// Instead of typedef - to allow forward declaration.
class BackgroundSchedulePoolLog : public SystemLog<BackgroundSchedulePoolLogElement>
{
    using SystemLog<BackgroundSchedulePoolLogElement>::SystemLog;
    size_t duration_milliseconds_threshold = 0;

public:
    void setDurationMillisecondsThreshold(size_t duration_milliseconds_threshold_) { duration_milliseconds_threshold = duration_milliseconds_threshold_; }
    size_t getDurationMillisecondsThreshold() const { return duration_milliseconds_threshold; }
};

}
