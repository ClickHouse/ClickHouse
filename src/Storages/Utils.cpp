#include <Storages/Utils.h>
#include <Storages/IStorage.h>


namespace CurrentMetrics
{
    extern const Metric AttachedTable;
    extern const Metric AttachedView;
    extern const Metric AttachedDictionary;
}


namespace DB
{
    CurrentMetrics::Metric getAttachedCounterForStorage(const StoragePtr & storage)
    {
        if (storage->isView())
        {
            return CurrentMetrics::AttachedView;
        }
        else if (storage->isDictionary())
        {
            return CurrentMetrics::AttachedDictionary;
        }
        else
        {
            return CurrentMetrics::AttachedTable;
        }
    }
}
