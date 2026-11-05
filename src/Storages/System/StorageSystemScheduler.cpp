#include <Storages/System/StorageSystemScheduler.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Nodes/FairPolicy.h>
#include <Common/Scheduler/Nodes/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/ThrottlerConstraint.h>
#include <Common/Scheduler/Nodes/FifoQueue.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemScheduler::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"resource", std::make_shared<DataTypeString>(), "Resource name"},
        {"path", std::make_shared<DataTypeString>(), "Path to a scheduling node within this resource scheduling hierarchy"},
        {"type", std::make_shared<DataTypeString>(), "Type of a scheduling node."},
        {"weight", std::make_shared<DataTypeFloat64>(), "Weight of a node, used by a parent node of `fair` type."},
        {"priority", std::make_shared<DataTypeInt64>(), "Priority of a node, used by a parent node of 'priority' type (Lower value means higher priority)."},
        {"is_active", std::make_shared<DataTypeUInt8>(), "Whether this node is currently active - has resource requests to be dequeued and constraints satisfied."},
        {"active_children", std::make_shared<DataTypeUInt64>(), "The number of children in active state."},
        {"dequeued_requests", std::make_shared<DataTypeUInt64>(), "The total number of resource requests dequeued from this node."},
        {"canceled_requests", std::make_shared<DataTypeUInt64>(), "The total number of resource requests canceled from this node."},
        {"dequeued_cost", std::make_shared<DataTypeInt64>(), "The sum of costs (e.g. size in bytes) of all requests dequeued from this node."},
        {"throughput", std::make_shared<DataTypeFloat64>(), "Current average throughput (dequeued cost per second)."},
        {"canceled_cost", std::make_shared<DataTypeInt64>(), "The sum of costs (e.g. size in bytes) of all requests canceled from this node."},
        {"busy_periods", std::make_shared<DataTypeUInt64>(), "The total number of deactivations of this node."},
        {"vruntime", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For children of `fair` nodes only. Virtual runtime of a node used by SFQ algorithm to select the next child to process in a max-min fair manner."},
        {"system_vruntime", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For `fair` nodes only. Virtual runtime showing `vruntime` of the last processed resource request. "
            "Used during child activation as the new value of `vruntime`."
        },
        {"queue_length", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For `fifo` nodes only. Current number of resource requests residing in the queue."
        },
        {"queue_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For fifo nodes only. Sum of costs (e.g. size in bytes) of all requests residing in the queue."
        },
        {"budget", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For fifo nodes only. The number of available 'cost units' for new resource requests. "
            "Can appear in case of discrepancy of estimated and real costs of resource requests (e.g. after read/write failure)"
        },
        {"is_satisfied", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "For constraint nodes only (e.g. `inflight_limit`). Equals to `1` if all the constraint of this node are satisfied."
        },
        {"inflight_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For `inflight_limit` nodes only. The number of resource requests dequeued from this node, that are currently in consumption state."
        },
        {"inflight_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For `inflight_limit` nodes only. "
            "The sum of costs (e.g. bytes) of all resource requests dequeued from this node, that are currently in consumption state."
        },
        {"max_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For `inflight_limit` nodes only. Upper limit for inflight_requests leading to constraint violation."
        },
        {"max_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For `inflight_limit` nodes only. Upper limit for inflight_cost leading to constraint violation."
        },
        {"max_speed", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For `bandwidth_limit` nodes only. Upper limit for bandwidth in tokens per second."
        },
        {"max_burst", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For `bandwidth_limit` nodes only. Upper limit for tokens available in token-bucket throttler."
        },
        {"throttling_us", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For `bandwidth_limit` nodes only. Total number of microseconds this node was in throttling state."
        },
        {"tokens", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For `bandwidth_limit` nodes only. Number of tokens currently available in token-bucket throttler."
        },
    };
}


void StorageSystemScheduler::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    context->getResourceManager()->forEachNode([&] (const String & resource, const String & path, ISchedulerNode * node)
    {
        size_t i = 0;
        res_columns[i++]->insert(resource);
        res_columns[i++]->insert(path);
        res_columns[i++]->insert(node->getTypeName());
        res_columns[i++]->insert(node->info.weight);
        res_columns[i++]->insert(node->info.priority.value);
        res_columns[i++]->insert(node->isActive());
        res_columns[i++]->insert(node->activeChildren());
        res_columns[i++]->insert(node->dequeued_requests.load());
        res_columns[i++]->insert(node->canceled_requests.load());
        res_columns[i++]->insert(node->dequeued_cost.load());
        res_columns[i++]->insert(node->throughput.rate(static_cast<double>(clock_gettime_ns())/1e9));
        res_columns[i++]->insert(node->canceled_cost.load());
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
            if (auto value = parent->getChildVRuntime(node))
                vruntime = *value;
        }
        if (auto * ptr = dynamic_cast<FairPolicy *>(node))
            system_vruntime = ptr->getSystemVRuntime();
        if (auto * ptr = dynamic_cast<FifoQueue *>(node))
            std::tie(queue_length, queue_cost) = ptr->getQueueLengthAndCost();
        if (auto * ptr = dynamic_cast<ISchedulerQueue *>(node))
            budget = ptr->getBudget();
        if (auto * ptr = dynamic_cast<ISchedulerConstraint *>(node))
            is_satisfied = ptr->isSatisfied();
        if (auto * ptr = dynamic_cast<SemaphoreConstraint *>(node))
        {
            std::tie(inflight_requests, inflight_cost) = ptr->getInflights();
            std::tie(max_requests, max_cost) = ptr->getLimits();
        }
        if (auto * ptr = dynamic_cast<ThrottlerConstraint *>(node))
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
