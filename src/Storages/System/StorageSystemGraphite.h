#pragma once

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Provides information about Graphite configuration.
class StorageSystemGraphite final : public IStorageSystemOneBlock<StorageSystemGraphite>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemGraphite> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemGraphite>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemGraphite(CreatePasskey, TArgs &&... args) : StorageSystemGraphite{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemGraphite"; }

    static NamesAndTypesList getNamesAndTypes();

    struct Config
    {
        Graphite::Params graphite_params;
        Array databases;
        Array tables;
    };

    using Configs = std::map<const String, Config>;


protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
