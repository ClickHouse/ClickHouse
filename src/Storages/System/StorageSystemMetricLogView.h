#pragma once

#include <Storages/StorageView.h>

namespace DB
{

class StorageSystemMetricLogView final : public StorageView
{
public:
    explicit StorageSystemMetricLogView(const StorageID & table_id_);

    std::string getName() const override { return "SystemMetricLogView"; }

    bool isSystemStorage() const override { return true; }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;
};

}
