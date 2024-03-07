#include <Storages/System/StorageSystemScheduler.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ISchedulerNode.h>
#include <IO/IResourceManager.h>
#include <IO/Resource/FairPolicy.h>
#include <IO/Resource/PriorityPolicy.h>
#include <IO/Resource/SemaphoreConstraint.h>
#include <IO/Resource/ThrottlerConstraint.h>
#include <IO/Resource/FifoQueue.h>
#include <Interpreters/Context.h>
#include "IO/ResourceRequest.h"


namespace DB
{

NamesAndTypesList StorageSystemScheduler::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"resource", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"weight", std::make_shared<DataTypeFloat64>()},
        {"priority", std::make_shared<DataTypeInt64>()},
        {"is_active", std::make_shared<DataTypeUInt8>()},
        {"active_children", std::make_shared<DataTypeUInt64>()},
        {"dequeued_requests", std::make_shared<DataTypeUInt64>()},
        {"dequeued_cost", std::make_shared<DataTypeInt64>()},
        {"busy_periods", std::make_shared<DataTypeUInt64>()},
        {"vruntime", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())},
        {"system_vruntime", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())},
        {"queue_length", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"queue_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"budget", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"is_satisfied", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
        {"inflight_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"inflight_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_speed", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())},
        {"max_burst", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())},
        {"throttling_us", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"tokens", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())},
    };
    return names_and_types;
}


void StorageSystemScheduler::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->getResourceManager()->forEachNode([&] (const String & resource, const String & path, const String & type, const SchedulerNodePtr & node)
    {
        size_t i = 0;
        res_columns[i++]->insert(resource);
        res_columns[i++]->insert(path);
        res_columns[i++]->insert(type);
        res_columns[i++]->insert(node->info.weight);
        res_columns[i++]->insert(node->info.priority.value);
        res_columns[i++]->insert(node->isActive());
        res_columns[i++]->insert(node->activeChildren());
        res_columns[i++]->insert(node->dequeued_requests.load());
        res_columns[i++]->insert(node->dequeued_cost.load());
        res_columns[i++]->insert(node->busy_periods.load());

        Field vruntime;
        Field system_vruntime;
        Field queue_length;
        Field queue_cost;
        Field budget;
        Field is_satisfied;
        Field inflight_requests;
        Field inflight_cost;
        Field max_requests;
        Field max_cost;
        Field max_speed;
        Field max_burst;
        Field throttling_us;
        Field tokens;

        if (auto * parent = dynamic_cast<FairPolicy *>(node->parent))
        {
            if (auto value = parent->getChildVRuntime(node.get()))
                vruntime = *value;
        }
        if (auto * ptr = dynamic_cast<FairPolicy *>(node.get()))
            system_vruntime = ptr->getSystemVRuntime();
        if (auto * ptr = dynamic_cast<FifoQueue *>(node.get()))
            std::tie(queue_length, queue_cost) = ptr->getQueueLengthAndCost();
        if (auto * ptr = dynamic_cast<ISchedulerQueue *>(node.get()))
            budget = ptr->getBudget();
        if (auto * ptr = dynamic_cast<ISchedulerConstraint *>(node.get()))
            is_satisfied = ptr->isSatisfied();
        if (auto * ptr = dynamic_cast<SemaphoreConstraint *>(node.get()))
        {
            std::tie(inflight_requests, inflight_cost) = ptr->getInflights();
            std::tie(max_requests, max_cost) = ptr->getLimits();
        }
        if (auto * ptr = dynamic_cast<ThrottlerConstraint *>(node.get()))
        {
            std::tie(max_speed, max_burst) = ptr->getParams();
            throttling_us = ptr->getThrottlingDuration().count() / 1000;
            tokens = ptr->getTokens();
        }

        res_columns[i++]->insert(vruntime);
        res_columns[i++]->insert(system_vruntime);
        res_columns[i++]->insert(queue_length);
        res_columns[i++]->insert(queue_cost);
        res_columns[i++]->insert(budget);
        res_columns[i++]->insert(is_satisfied);
        res_columns[i++]->insert(inflight_requests);
        res_columns[i++]->insert(inflight_cost);
        res_columns[i++]->insert(max_requests);
        res_columns[i++]->insert(max_cost);
        res_columns[i++]->insert(max_speed);
        res_columns[i++]->insert(max_burst);
        res_columns[i++]->insert(throttling_us);
        res_columns[i++]->insert(tokens);
    });
}

}
