#include "Storages/MergeTree/MergeTask.h"

namespace DB
{

bool MergeTaskChain::execute()
{
    if (tasks.front()->execute())
        return true;

    tasks.pop_front();
    return !tasks.empty();
}


void MergeTaskChain::add(MergeTaskPtr task)
{
    tasks.emplace_back(task);
}

}
