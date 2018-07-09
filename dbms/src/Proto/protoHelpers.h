#pragma once


namespace DB
{
    class Context;
    class Block;

    Block storeContextBlock(Context & context);
    void loadContextBlock(const Block & block, Context & context);
}
