#include <Columns/IColumn.h>
#include <IO/ReadHelpers.h>
#include <Common/QuillLogger.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemLoggerThreads.h>

#include <quill/core/ThreadContextManager.h>

namespace DB
{

ColumnsDescription StorageSystemLoggerThreads::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread ID."},
        {"queue_allocated", std::make_shared<DataTypeUInt64>(), "Capacity of thread's logger queue."},
    };
}

void StorageSystemLoggerThreads::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    quill::detail::ThreadContextManager::instance().for_each_thread_context(
        [&](quill::detail::ThreadContext * thread_context)
        {
            res_columns[0]->insert(DB::parse<uint64_t>(thread_context->thread_id()));
            res_columns[1]->insert(thread_context->template get_spsc_queue<DB::QuillFrontendOptions::queue_type>().total_allocated());
        });
}

}
