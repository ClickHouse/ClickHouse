#pragma once
#include <Core/Types.h>

/// Returns persistent UUID of current clickhouse-server or clickhouse-keeper instance.
DB::UUID getServerUUID();
