#pragma once


namespace DB
{
    class Context;
    struct ColumnWithTypeAndName;

    ColumnWithTypeAndName storeContext(Context & context);
    void loadContext(const ColumnWithTypeAndName & proto_column, Context & context);
}
