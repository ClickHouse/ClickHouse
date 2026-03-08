#include <Storages/System/StorageSystemScheduler.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/Nodes/TimeShared/FairPolicy.h>
#include <Common/Scheduler/Nodes/TimeShared/PriorityPolicy.h>
#include <Common/Scheduler/Nodes/TimeShared/SemaphoreConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/ThrottlerConstraint.h>
#include <Common/Scheduler/Nodes/TimeShared/FifoQueue.h>
#include <Common/Scheduler/Nodes/SpaceShared/AllocationQueue.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription StorageSystemScheduler::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"resource", std::make_shared<DataTypeString>(), "Resource name"},

        // ISchedulerNode (common for all nodes)
        {"path", std::make_shared<DataTypeString>(), "Path to a scheduling node within this resource scheduling hierarchy"},
        {"type", std::make_shared<DataTypeString>(), "Type of a scheduling node."},
        {"weight", std::make_shared<DataTypeFloat64>(), "Weight of a node, used by a parent node of `fair` type."},
        {"priority", std::make_shared<DataTypeInt64>(), "Priority of a node, used by a parent node of 'priority' type (Lower value means higher priority)."},

        // ITimeSharedNode
        {"is_active", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "Whether this node is currently active - has resource requests to be dequeued and constraints satisfied."},
        {"active_children", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The number of children in active state."},
        {"dequeued_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The total number of resource requests dequeued from this node."},
        {"canceled_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The total number of resource requests canceled from this node."},
        {"rejected_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The total number of resource requests rejected from this node."},
        {"dequeued_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "The sum of costs (e.g. size in bytes) of all requests dequeued from this node."},
        {"canceled_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "The sum of costs (e.g. size in bytes) of all requests canceled from this node."},
        {"rejected_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "The sum of costs (e.g. size in bytes) of all requests rejected from this node."},
        {"busy_periods", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The total number of deactivations of this node."},
        {"throughput", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "Current average throughput (dequeued cost per second)."},

        // FairPolicy
        {"vruntime", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For children of `fair` nodes only. Virtual runtime of a node used by SFQ algorithm to select the next child to process in a max-min fair manner."},
        {"system_vruntime", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
            "For `fair` nodes only. Virtual runtime showing `vruntime` of the last processed resource request. "
            "Used during child activation as the new value of `vruntime`."
        },

        // FifoQueue and AllocationQueue
        {"queue_length", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For `fifo` nodes only. Current number of resource requests residing in the queue."
        },
        {"queue_cost", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For fifo nodes only. Sum of costs (e.g. size in bytes) of all requests residing in the queue."
        },

        // FifoQueue
        {"budget", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For fifo nodes only. The number of available 'cost units' for new resource requests. "
            "Can appear in case of discrepancy of estimated and real costs of resource requests (e.g. after read/write failure)"
        },

        // ISchedulerConstraint
        {"is_satisfied", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "For constraint nodes only (e.g. `inflight_limit`). Equals to `1` if all the constraint of this node are satisfied."
        },

        // SemaphoreConstraint
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

        // ThrottlerConstraint
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

        // ISpaceSharedNode
        {"allocated", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            "For space-shared nodes only. The currently allocated amount of resource under this node."
        },
        {"allocations", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The current number of running resource allocations under this node."
        },
        {"updates", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of updates propagated through this node."
        },
        {"increases", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of increase requests approved by this node."
        },
        {"decreases", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of decrease requests approved by this node."
        },
        {"admits", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of allocations admitted by this node."
        },
        {"removes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of allocations removed from this node."
        },
        {"killers", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of evictions initiated due to increase requests by this node."
        },
        {"victims", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For space-shared nodes only. The total number of allocations that were evicted from this node."
        },
        // AllocationQueue
        {"rejects", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For `allocation_queue` nodes only. The total number of resource allocations rejected from this node."
        },
        {"pending", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "For `allocation_queue` nodes only. The current number of pending resource allocations in this node."
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

        Field is_active;
        Field active_children;
        Field dequeued_requests;
        Field canceled_requests;
        Field rejected_requests;
        Field dequeued_cost;
        Field canceled_cost;
        Field rejected_cost;
        Field busy_periods;
        Field throughput;
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
        Field allocated;
        Field allocations;
        Field updates;
        Field increases;
        Field decreases;
        Field admits;
        Field removes;
        Field killers;
        Field victims;
        Field rejects;
        Field pending;

        if (auto * ptr = dynamic_cast<ITimeSharedNode *>(node))
        {
            is_active = ptr->isActive();
            active_children = ptr->activeChildren();
            dequeued_requests = ptr->dequeued_requests.load();
            canceled_requests = ptr->canceled_requests.load();
            rejected_requests = ptr->rejected_requests.load();
            dequeued_cost = ptr->dequeued_cost.load();
            canceled_cost = ptr->canceled_cost.load();
            rejected_cost = ptr->rejected_cost.load();
            busy_periods = ptr->busy_periods.load();
            throughput = ptr->throughput.rate(static_cast<double>(clock_gettime_ns())/1e9);
        }
        if (auto * parent = dynamic_cast<FairPolicy *>(node->parent))
        {
            if (auto value = parent->getChildVRuntime(node))
                vruntime = *value;
        }
        if (auto * ptr = dynamic_cast<FairPolicy *>(node))
            system_vruntime = ptr->getSystemVRuntime();
        if (auto * ptr = dynamic_cast<FifoQueue *>(node))
            std::tie(queue_length, queue_cost) = ptr->getQueueLengthAndCost();
        if (auto * ptr = dynamic_cast<AllocationQueue *>(node))
            std::tie(queue_length, queue_cost) = ptr->getQueueLengthAndSize();
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
        if (auto * ptr = dynamic_cast<ISpaceSharedNode *>(node))
        {
            allocated = ptr->allocated;
            allocations = ptr->allocations;
            updates = ptr->updates;
            increases = ptr->increases;
            decreases = ptr->decreases;
            admits = ptr->admits;
            removes = ptr->removes;
            killers = ptr->killers;
            victims = ptr->victims;
        }
        if (auto * ptr = dynamic_cast<AllocationQueue *>(node))
        {
            rejects = ptr->getRejects();
            pending = ptr->getPending();
        }

        res_columns[i++]->insert(is_active);
        res_columns[i++]->insert(active_children);
        res_columns[i++]->insert(dequeued_requests);
        res_columns[i++]->insert(canceled_requests);
        res_columns[i++]->insert(rejected_requests);
        res_columns[i++]->insert(dequeued_cost);
        res_columns[i++]->insert(canceled_cost);
        res_columns[i++]->insert(rejected_cost);
        res_columns[i++]->insert(busy_periods);
        res_columns[i++]->insert(throughput);
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
        res_columns[i++]->insert(allocated);
        res_columns[i++]->insert(allocations);
        res_columns[i++]->insert(updates);
        res_columns[i++]->insert(increases);
        res_columns[i++]->insert(decreases);
        res_columns[i++]->insert(admits);
        res_columns[i++]->insert(removes);
        res_columns[i++]->insert(killers);
        res_columns[i++]->insert(victims);
        res_columns[i++]->insert(rejects);
        res_columns[i++]->insert(pending);
    });
}

}
