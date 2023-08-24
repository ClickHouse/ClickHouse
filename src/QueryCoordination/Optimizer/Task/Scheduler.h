#pragma once

#include <QueryCoordination/Optimizer/Task/Task.h>

namespace DB
{

class Scheduler
{
public:
    void run()
    {
        while (!stack.empty())
        {
            auto task = std::move(stack.top());
            stack.pop();

            task->execute();
        }
    }

    void pushTask(std::unique_ptr<Task> task)
    {
        stack.push(std::move(task));
    }

    std::stack<std::unique_ptr<Task>> & taskStack()
    {
        return stack;
    }

private:
    std::stack<std::unique_ptr<Task>> stack;
};

}
