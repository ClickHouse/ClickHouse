#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndAliases.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

#include <cstdint>

namespace DB
{

struct LLMQueryLogElement
{
    enum Type : int8_t
    {
        RequestStart = 1,
        RequestFinish = 2,
        RequestException = 3,
    };

    time_t event_time = 0;
    Decimal64 event_time_microseconds{};
    Type type = RequestStart;
    UUID request_id{};
    String query_id;
    String initial_query_id;
    String user;
    String route;
    String deployment;
    String model;
    UInt64 config_version = 0;
    String prompt;
    String system_prompt;
    UInt64 max_tokens = 0;
    Float64 temperature = 0;
    String response;
    String finish_reason;
    UInt64 input_tokens = 0;
    UInt64 output_tokens = 0;
    UInt64 duration_microseconds = 0;
    Int32 exception_code = 0;

    static std::string name()
    {
        return "LLMQueryLog";
    }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases()
    {
        return {};
    }
    void appendToBlock(MutableColumns & columns) const;
};

class LLMQueryLog : public SystemLog<LLMQueryLogElement>
{
    using SystemLog<LLMQueryLogElement>::SystemLog;
};

}
