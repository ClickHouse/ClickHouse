#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemParts final : public StorageSystemPartsBase, boost::noncopyable
{
public:
    explicit StorageSystemParts(const StorageID & table_id_);

    std::string getName() const override { return "SystemParts"; }

protected:
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
