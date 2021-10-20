#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/// Adds defaults to columns using BlockDelayedDefaults bitmask attached to Block by child InputStream.
class AddingDefaultsBlockInputStream : public IBlockInputStream
{
public:
    AddingDefaultsBlockInputStream(
        const BlockInputStreamPtr & input,
        const ColumnsDescription & columns_,
        ContextPtr context_);

    String getName() const override { return "AddingDefaults"; }
    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Block header;
    const ColumnsDescription columns;
    const ColumnDefaults column_defaults;
    ContextPtr context;
};

}
