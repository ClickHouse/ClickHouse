#pragma once


namespace DB
{

class Context;

/// Load tables and add them to context.
void loadMetadata(Context & context);

}
