#pragma once

namespace DB
{
    class Context;
    class Block;
    class TableMetadata;

    Block storeContextBlock(Context & context);
    void loadTableMetadata(const Block & block, TableMetadata & table_meta);
}
