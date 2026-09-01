#pragma once

namespace DB
{

class SchedulerNodeFactory;

void registerPriorityPolicy(SchedulerNodeFactory &);
void registerFairPolicy(SchedulerNodeFactory &);
void registerSemaphoreConstraint(SchedulerNodeFactory &);
void registerThrottlerConstraint(SchedulerNodeFactory &);
void registerFifoQueue(SchedulerNodeFactory &);

void registerSchedulerNodes();

}
