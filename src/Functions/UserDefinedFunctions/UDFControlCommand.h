#ifndef CLICKHOUSE_UDFCONTROLCOMMAND_H
#define CLICKHOUSE_UDFCONTROLCOMMAND_H

#include <string>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

struct UDFControlCommand
{
    static constexpr auto InitLib = "InitLib";
    static constexpr auto ExecFunc = "ExecFunc";
    static constexpr auto GetReturnType = "GetReturnType";
    static constexpr auto Stop = "Stop";


    std::string name;
    std::vector<std::string> args;
    unsigned request_id;

    void read(ReadBuffer & in)
    {
        readVarUInt(request_id, in);
        readStringBinary(name, in);
        size_t args_num;
        readVarUInt(args_num, in);
        args.resize(args_num);
        for (auto && arg : args)
        {
            readStringBinary(arg, in);
        }
    }

    void write(WriteBuffer & out)
    {
        writeVarUInt(request_id, out);
        writeStringBinary(name, out);
        writeVarUInt(args.size(), out);
        for (auto && arg : args)
        {
            writeStringBinary(arg, out);
        }
        out.sync();
    }
};

struct UDFControlCommandResult
{
    unsigned code;
    std::string message; // Contains error message on error
    unsigned request_id;

    bool isSuccess() { return code == 0; }

    void read(ReadBuffer & in)
    {
        readVarUInt(request_id, in);
        readVarUInt(code, in);
        readStringBinary(message, in);
    }

    void write(WriteBuffer & out)
    {
        writeVarUInt(request_id, out);
        writeVarUInt(code, out);
        writeStringBinary(message, out);
        out.sync();
    }
};

}

#endif //CLICKHOUSE_UDFCONTROLCOMMAND_H
