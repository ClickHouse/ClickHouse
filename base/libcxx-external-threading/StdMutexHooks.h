#pragma once

struct StdMutexHooks
{
    void (*before_lock)();
    void (*after_lock)();
    void (*before_unlock)();
    void (*after_unlock)();
};

void setStdMutexHooks(const StdMutexHooks * hooks);
