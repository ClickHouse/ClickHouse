#pragma once
#if USE_CAPNP

namespace DB
{
    class Context;
    class Block;
    struct TableMetadata;

    Block storeTableMetadata(const TableMetadata & table_meta);
    void loadTableMetadata(const Block & block, TableMetadata & table_meta);
}

#endif
