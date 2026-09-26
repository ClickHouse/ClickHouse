#include <Interpreters/TransactionPayloads.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace DB::Tx
{

namespace
{
    /// Append " <label>name1,name2,...". `label` already carries its '='. No-op for an empty list.
    void appendLabelledList(WriteBuffer & buf, const char * label, const std::vector<String> & names)
    {
        if (names.empty())
            return;
        writeChar(' ', buf);
        writeString(label, buf);
        for (size_t i = 0; i < names.size(); ++i)
        {
            if (i > 0)
                writeChar(',', buf);
            writeString(names[i], buf);
        }
    }

    /// Read the labelled sub-lists that follow a table line's `cross_replica_id`, in any
    /// order, stopping at '\n' or EOF. Unknown labels throw.
    void parseAffectedRowLists(
        ReadBuffer & buf,
        String & out_zk_path,
        std::vector<String> & out_added,
        std::vector<String> & out_removed)
    {
        while (!buf.eof() && *buf.position() == ' ')
        {
            buf.ignore();
            char label = *buf.position();
            std::vector<String> * target = nullptr;
            if (label == 'P')
            {
                buf.ignore();
                assertChar('=', buf);
                while (!buf.eof() && *buf.position() != '\n' && *buf.position() != ' ')
                {
                    out_zk_path.push_back(*buf.position());
                    buf.ignore();
                }
                continue;
            }
            if (label == 'A')
                target = &out_added;
            else if (label == 'R')
                target = &out_removed;
            else
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
                    "Unknown CSN entry sub-list label '{}'", label);
            buf.ignore();
            assertChar('=', buf);
            while (!buf.eof() && *buf.position() != '\n' && *buf.position() != ' ')
            {
                String name;
                while (!buf.eof() && *buf.position() != ',' && *buf.position() != '\n' && *buf.position() != ' ')
                {
                    name.push_back(*buf.position());
                    buf.ignore();
                }
                target->push_back(std::move(name));
                if (!buf.eof() && *buf.position() == ',')
                    buf.ignore();
            }
        }
    }
}

String CSNEntryData::serialize() const
{
    WriteBufferFromOwnString buf;
    writeString("version: 1\n", buf);
    TransactionID::write(tid, buf);
    writeChar('\n', buf);
    writeString("replica_id: ", buf);
    writeText(replica_id, buf);
    writeChar('\n', buf);
    writeString("smt_count: ", buf);
    writeText(smt.size(), buf);
    for (const auto & row : smt)
    {
        writeChar('\n', buf);
        writeText(row.cross_replica_id, buf);
        if (!row.zk_path.empty())
        {
            writeChar(' ', buf);
            writeString("P=", buf);
            writeString(row.zk_path, buf);
        }
        appendLabelledList(buf, "A=", row.added_part_names);
        appendLabelledList(buf, "R=", row.removed_part_names);
    }
    return buf.str();
}

CSNEntryData CSNEntryData::deserialize(const String & data)
{
    CSNEntryData result;
    if (data.empty())
        return result;

    ReadBufferFromString buf{data};
    /// Pre-version-1 znodes hold just the bare TID text and carry no replica_id or SMT fan-out.
    if (!checkString("version: ", buf))
    {
        result.tid = TransactionID::read(buf);
        assertEOF(buf);
        return result;
    }
    UInt64 version = 0;
    readText(version, buf);
    assertChar('\n', buf);
    if (version != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unknown CSN entry format version: {}", version);
    result.format_version = version;
    result.tid = TransactionID::read(buf);
    assertChar('\n', buf);

    assertString("replica_id: ", buf);
    readText(result.replica_id, buf);
    assertChar('\n', buf);

    assertString("smt_count: ", buf);
    size_t smt_count = 0;
    readText(smt_count, buf);
    result.smt.reserve(smt_count);
    for (size_t i = 0; i < smt_count; ++i)
    {
        assertChar('\n', buf);
        MergeTreeTransaction::AffectedSMTTable row{};
        readText(row.cross_replica_id, buf);
        parseAffectedRowLists(buf, row.zk_path, row.added_part_names, row.removed_part_names);
        result.smt.push_back(std::move(row));
    }
    assertEOF(buf);
    return result;
}

String StampData::serialize() const
{
    WriteBufferFromOwnString buf;
    writeString("version: 1\n", buf);
    TransactionID::write(tid, buf);
    if (!zk_path.empty())
    {
        writeChar('\n', buf);
        writeString(zk_path, buf);
    }
    return buf.str();
}

StampData StampData::deserialize(const String & data)
{
    StampData result;
    if (data.empty())
        return result;

    ReadBufferFromString buf{data};
    assertString("version: ", buf);
    UInt64 version = 0;
    readText(version, buf);
    assertChar('\n', buf);
    if (version != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unknown stamp data format version: {}", version);
    result.format_version = version;
    result.tid = TransactionID::read(buf);
    if (!buf.eof() && *buf.position() == '\n')
    {
        buf.ignore();
        readStringUntilEOF(result.zk_path, buf);
    }
    return result;
}

String ProcessedData::serialize() const
{
    WriteBufferFromOwnString buf;
    writeString("version: 1\n", buf);
    writeText(csn, buf);
    writeChar('\n', buf);
    TransactionID::write(tid, buf);
    writeChar('\n', buf);
    writeText(virtual_parts_version, buf);
    writeChar('\n', buf);
    writeText(writing_replica_id, buf);
    return buf.str();
}

ProcessedData ProcessedData::deserialize(const String & data)
{
    ProcessedData result;
    if (data.empty())
        return result;

    ReadBufferFromString buf{data};
    assertString("version: ", buf);
    UInt64 version = 0;
    readText(version, buf);
    assertChar('\n', buf);
    if (version != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unknown processed data format version: {}", version);
    result.format_version = version;
    readText(result.csn, buf);
    assertChar('\n', buf);
    result.tid = TransactionID::read(buf);
    assertChar('\n', buf);
    readText(result.virtual_parts_version, buf);
    assertChar('\n', buf);
    readText(result.writing_replica_id, buf);
    return result;
}

String serializeCSN(CSN csn)
{
    /// Keeper pads to a minimum of ten digits and its counter is `int64_t`, so names grow past
    /// 10^10 and stop being order-comparable as strings. `zkutil::getSequentialNodeName` assumes
    /// Int32 and underflows its padding there.
    const String num = std::to_string(csn);
    return num.size() >= 10 ? "csn-" + num : "csn-" + String(10 - num.size(), '0') + num;
}

UInt64 deserializeCSN(const String & csn_node_name)
{
    ReadBufferFromString buf{csn_node_name};
    assertString("csn-", buf);
    UInt64 res = 0;
    readText(res, buf);
    assertEOF(buf);
    return res;
}

}
