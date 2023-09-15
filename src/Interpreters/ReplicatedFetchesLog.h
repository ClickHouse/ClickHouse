#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
class Counters;
}

namespace DB
{

struct ReplicatedFetchLogElement : public ReplicatedFetchInfo
{
    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;
    ReplicatedFetchInfo info;
    UInt64 exception_code = 0;
    String exception;

    static std::string name() { return "ReplicatedFetchesLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class IMergeTreeDataPart;


/// Instead of typedef - to allow forward declaration.
class ReplicatedFetchesLog : public SystemLog<ReplicatedFetchLogElement>
{
    using SystemLog<ReplicatedFetchLogElement>::SystemLog;

public:
    static bool addNewEntry(const ContextPtr & current_context, const ReplicatedFetchInfo & info, const ExecutionStatus & es);
};

}
