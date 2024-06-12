#pragma once

#include "config.h"

#if USE_RDKAFKA


#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class StorageSystemKafkaConsumers final : public IStorageSystemOneBlock<StorageSystemKafkaConsumers>
{
public:
    std::string getName() const override { return "SystemKafkaConsumers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}

#endif
