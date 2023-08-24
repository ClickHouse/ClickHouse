#pragma once

#include <memory>
#include <stack>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Memo;

class Task
{
public:
    Task(Memo & memo_, std::stack<std::unique_ptr<Task>> & stack_, ContextPtr context_) : memo(memo_), stack(stack_), context(context_) {}

    virtual ~Task() = default;

    virtual void execute();

protected:
    void pushTask(std::unique_ptr<Task> task)
    {
        stack.push(std::move(task));
    }

    Memo & memo;

    std::stack<std::unique_ptr<Task>> & stack;

    ContextPtr context;
};

}
