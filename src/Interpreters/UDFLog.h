#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/Field.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/SystemLog.h>

namespace DB
{

struct UDFLogElement
{
    String function_name;
    time_t event_time{};
    UInt64 number_of_processed_rows{};
    UInt64 number_of_processed_bytes{};
    UInt64 duration_ms{};
    Array constant_function_args;

    static std::string name() { return "UDFLog";}
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class UDFLog: public SystemLog<UDFLogElement>
{
    using SystemLog<UDFLogElement>::SystemLog;
};

}
