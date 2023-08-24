#pragma once

#include <QueryCoordination/Optimizer/Task/Task.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{


class Memo;
class Group;
class PhysicalProperties;

class EnforceCostGroup final : public Task
{
public:
    EnforceCostGroup(
        Memo & memo_,
        std::stack<std::unique_ptr<Task>> & stack_,
        ContextPtr context_,
        Group & group_,
        const PhysicalProperties & required_prop_)
        : Task(memo_, stack_, context_), group(group_), required_prop(required_prop_)
    {
    }

    void execute() override;

private:
    Group & group;
    const PhysicalProperties & required_prop;
};

}
